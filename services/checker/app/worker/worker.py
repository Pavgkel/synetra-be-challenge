import os
import json
import socket
import struct
import sqlite3
import hashlib
import base64
from threading import Thread, Lock
import pika
from minio import Minio

# Конфигурация путей и портов
DB_PATH = os.getenv("DB_PATH", "/app/data/sqlite/excercise.db")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")

# Подтягиваем твои реальные креды из .env
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "admin12345")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "admin12345")

RMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "admin")
RMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "admin")
# Формируем правильный внутренний URL для Docker-сети
RABBITMQ_URL = os.getenv("RABBITMQ_URL", f"amqp://{RMQ_USER}:{RMQ_PASS}@rabbitmq:5672/")

# Потокобезопасный список активных WS-клиентов
ws_clients = set()
clients_lock = Lock()

def get_image_size(file_path):
    """Определяет ширину и высоту PNG/JPEG без Pillow."""
    with open(file_path, 'rb') as f:
        head = f.read(24)
        if head.startswith(b'\x89PNG\r\n\x1a\n'):
            w, h = struct.unpack('>ii', head[16:24])
            return int(w), int(h)
        elif head.startswith(b'\xff\xd8'):
            f.seek(0)
            size = 2
            ftype = 0
            while ftype != 0xda:
                while ftype != 0xff: ftype = ord(f.read(1))
                while ftype == 0xff: ftype = ord(f.read(1))
                if 0xc0 <= ftype <= 0xc3:
                    f.read(3)
                    h, w = struct.unpack('>HH', f.read(4))
                    return int(w), int(h)
                else:
                    f.read(struct.unpack('>H', f.read(2)) - 2)
                ftype = ord(f.read(1))
    return 800, 600

def send_ws_frame(sock, message):
    """Формирует и отправляет текстовый фрейм WebSocket (RFC 6455)."""
    data = message.encode('utf-8')
    length = len(data)
    frame = bytearray([0x81]) # FIN=1, Opcode=1 (Text)
    if length <= 125:
        frame.append(length)
    elif length <= 65535:
        frame.append(126)
        frame.extend(struct.pack('!H', length))
    else:
        frame.append(127)
        frame.extend(struct.pack('!Q', length))
    frame.extend(data)
    try:
        sock.sendall(frame)
    except Exception:
        return False
    return True

def broadcast_ws(message):
    """Рассылает сообщение всем подключенным WebSocket клиентам."""
    with clients_lock:
        to_remove = set()
        for client in ws_clients:
            if not send_ws_frame(client, message):
                to_remove.add(client)
        ws_clients.difference_update(to_remove)

def handle_pipeline_event():
    """Основная бизнес-логика при получении сигнала от PLC."""
    img_dir = "/app/data/images/"
    if not os.path.exists(img_dir):
        print(f"Каталог {img_dir} не найден")
        return

    files = [f for f in os.listdir(img_dir) if f.lower().endswith(('.png', '.jpg', '.jpeg'))]
    if not files:
        print("В data/images/ нет изображений для обработки")
        return

    filename = files[0] # Берем первую картинку в качестве симуляции кадра
    filepath = os.path.join(img_dir, filename)
    w, h = get_image_size(filepath)

    try:
        # 1. Загрузка в MinIO
        minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)
        if not minio_client.bucket_exists("images"):
            minio_client.make_bucket("images")
        object_name = f"cam_{filename}"
        minio_client.fput_object("images", object_name, filepath)
        img_url = f"http://localhost:9000/images/{object_name}"

        # 2. Сохранение в SQLite
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO images (image_url, width, height) VALUES (?, ?, ?)", (img_url, w, h))
            conn.commit()

        payload = json.dumps({"image_url": img_url, "width": w, "height": h})
        print(f"🚀 Сквозное событие обработано: {payload}")

        # 3. Отправка в WebSockets (Фронтенду)
        broadcast_ws(payload)
                # 4. Отправка в RabbitMQ (очередь inference.in) с защитой от прогрева сокета
        credentials = pika.PlainCredentials(RMQ_USER, RMQ_PASS)
        params = pika.ConnectionParameters(
            host='rabbitmq',
            port=5672,
            virtual_host='/',
            credentials=credentials,
            heartbeat=60,             # Защита от разрыва соединения при простое
            blocked_connection_timeout=300
        )

        rmq_conn = None
        max_attempts = 5
        attempt_delay = 3

        print("📬 [RabbitMQ] Начинаем процесс отправки...")
        for attempt in range(1, max_attempts + 1):
            try:
                # Пытаемся открыть соединение
                rmq_conn = pika.BlockingConnection(params)
                channel = rmq_conn.channel()
                
                # Принудительно объявляем точку обмена и очередь
                channel.exchange_declare(exchange='inference.in', exchange_type='direct', durable=True)
                channel.queue_declare(queue='inference.in', durable=True)
                channel.queue_bind(exchange='inference.in', queue='inference.in', routing_key='inference.in')
                
                # Публикуем сообщение
                channel.basic_publish(
                    exchange='inference.in',
                    routing_key='inference.in',
                    body=payload
                )
                rmq_conn.close()
                print("✅ [RabbitMQ] Сообщение успешно доставлено в брокер (inference.in)!")
                break # Выходим из цикла, всё прошло успешно!
                
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as rmq_err:
                print(f"⏳ [RabbitMQ] Попытка {attempt}/{max_attempts} не удалась (брокер еще не готов). Повтор через {attempt_delay} сек...")
                if attempt == max_attempts:
                    print(f"❌ [RabbitMQ] Ошибка отправки после {max_attempts} попыток: {type(rmq_err).__name__}")
                else:
                    import time
                    time.sleep(attempt_delay)
            except Exception as unknown_err:
                print(f"⚠️ [RabbitMQ] Непредвиденная ошибка пайплайна: {type(unknown_err).__name__} - {unknown_err}")
                break


    

    except Exception as e:
        print(f"❌ Ошибка в пайплайне обработки: {e}")

class CustomTcpServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def run(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"📡 TCP Сервер запущен на {self.host}:{self.port} (Ожидание PLC...)")

        while True:
            client_sock, addr = server.accept()
            Thread(target=self.handle_client, args=(client_sock,)).start()

    def handle_client(self, sock):
        sock.settimeout(5.0) # Защита: если PLC зависнет, через 5 сек закроем сокет
        buffer = b""
        try:
            while True:
                data = sock.recv(1024)
                if not data:
                    break
                buffer += data
                
                # Проверяем, прилетел ли полный пакет команды
                if b"[s][save_image][e]" in buffer:
                    print("🎯 [TCP Server] Получена команда от PLC!")
                    
                    # Запускаем тяжелую обработку (MinIO, SQLite, RabbitMQ) 
                    # в отдельном независимом потоке, чтобы мгновенно освободить TCP
                    Thread(target=handle_pipeline_event).start()
                    
                    # Отправляем ответ клиенту (nc / PLC), что команда принята
                    sock.sendall(b"OK\n")
                    break # ВЫХОДИМ из цикла чтения, так как задача выполнена!
                    
        except Exception as e:
            print(f"⚠️ [TCP Server] Ошибка при обработке сокета: {e}")
        finally:
            sock.close() # Закрываем соединение, что заставит утилиту nc завершиться


class CustomWebSocketServer:
    """Легковесный WebSocket-сервер на чистых сокетах без сторонних зависимостей."""
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def run(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"🌐 WebSocket Сервер запущен на {self.host}:{self.port} (Ожидание Фронтенда...)")

        while True:
            client_sock, addr = server.accept()
            Thread(target=self.handle_handshake, args=(client_sock,)).start()

    def handle_handshake(self, sock):
        sock.settimeout(5.0)  # Защита от зависания потока
        try:
            # Читаем сырые данные запроса
            raw_request = sock.recv(2048)
            if not raw_request:
                sock.close()
                return
                
            request = raw_request.decode('utf-8', errors='ignore')
            print(f"📥 [WS Server] Получен HTTP запрос:\n{request}")  # ОТЛАДКА: посмотрим, что прислал клиент

            if "upgrade: websocket" not in request.lower():
                print("❌ [WS Server] В запросе отсутствует заголовок Upgrade: websocket")
                sock.close()
                return

            # Ищем ключ Sec-WebSocket-Key максимально надежно
            key = None
            for line in request.split("\r\n"):
                if line.lower().startswith("sec-websocket-key:"):
                    # Разбиваем строго по первому двоеточию и очищаем от пробелов
                    parts = line.split(":", 1)
                    if len(parts) > 1:
                        key = parts[1].strip()
                    break

            if key:
                print(f"🔑 [WS Server] Найден ключ авторизации: {key}")
                # Магическая строка по стандарту RFC 6455
                guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                accept_key = base64.b64encode(hashlib.sha1((key + guid).encode('utf-8')).digest()).decode('utf-8')
                
                # Формируем строгий HTTP-ответ для aiohttp
                response = (
                    "HTTP/1.1 101 Switching Protocols\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    f"Sec-WebSocket-Accept: {accept_key}\r\n"
                    "\r\n" # Обязательный пустой перевод строки в конце заголовков
                )
                sock.sendall(response.encode('utf-8'))
                
                # Переключаем сокет в бесконечный режим ожидания для удержания
                sock.settimeout(None)
                with clients_lock:
                    ws_clients.add(sock)
                print("✅ [WS Server] Рукопожатие завершено! Клиент успешно подключен.")
                
                # Удерживаем поток, пока клиент сам не разорвет соединение
                while True:
                    data = sock.recv(1024)
                    if not data:
                        break
            else:
                print("❌ [WS Server] Ошибка: заголовок Sec-WebSocket-Key не найден!")
                
        except Exception as e:
            print(f"⚠️ [WS Server] Исключение во время handshake: {e}")
        finally:
            with clients_lock:
                ws_clients.discard(sock)
            sock.close()



class Worker:
  def __init__(self, tcp_host: str, tcp_port: int, ws_host: str, ws_port: int) -> None:
    # Инвертируем роли: Чеккер запускает СЕРВЕРЫ на интерфейсе 0.0.0.0, чтобы Docker пускал трафик извне
    self._tcp_server = CustomTcpServer("0.0.0.0", tcp_port)
    self._ws_server = CustomWebSocketServer("0.0.0.0", ws_port)

  def run(self):
    tcp_thread = Thread(target=self._tcp_server.run)
    ws_thread = Thread(target=self._ws_server.run)

    tcp_thread.start()
    ws_thread.start()

    tcp_thread.join()
    ws_thread.join()