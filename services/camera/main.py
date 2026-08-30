import os
import sys
import json
import struct
import asyncio
import sqlite3
import aio_pika
from aiohttp import web
from minio import Minio

TCP_PORT = int(os.getenv("TCP_PORT", 9000))
WS_PORT = int(os.getenv("WS_PORT", 8001))
DB_PATH = os.getenv("DB_PATH", "/app/data/sqlite/excercise.db")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

# Подключаемся к MinIO (бакет "images" уже создан контейнером mc)
minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)

ws_clients = set()

def get_image_size(file_path):
    """Определяет разрешение изображения без сторонних библиотек типа Pillow."""
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

async def process_image():
    img_dir = "/app/data/images/"
    if not os.path.exists(img_dir):
        print(f"Directory {img_dir} does not exist")
        return

    files = [f for f in os.listdir(img_dir) if f.lower().endswith(('.png', '.jpg', '.jpeg'))]
    if not files:
        print("No images found in data/images/")
        return
    
    filename = files[0]
    filepath = os.path.join(img_dir, filename)
    w, h = get_image_size(filepath)
    
    # 1. Загрузка в MinIO (в бакет images)
    object_name = f"cam_{filename}"
    minio_client.fput_object("images", object_name, filepath)
    
    # URL для внешнего доступа через проброшенный порт Minio
    img_url = f"http://localhost:9000/images/{object_name}"
    
    # 2. Сохранение в SQLite
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO images (image_url, width, height) VALUES (?, ?, ?)", 
            (img_url, w, h)
        )
        conn.commit()
    
    payload = json.dumps({"image_url": img_url, "width": w, "height": h})
    print(f"📡 Event triggered: {payload}")

    # 3. Отправка в WebSockets подключенным клиентам
    for ws in list(ws_clients):
        if not ws.closed:
            await ws.send_str(payload)

    # 4. Отправка в RabbitMQ (используем direct exchange из твоего конфигуратора rmq)
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        async with connection:
            channel = await connection.channel()
            # exchange 'inference.in' объявлен как direct в docker-compose
            exchange = await channel.declare_exchange('inference.in', aio_pika.ExchangeType.DIRECT, durable=True)
            await exchange.publish(
                aio_pika.Message(body=payload.encode()),
                routing_key='inference.in'
            )
    except Exception as e:
        print(f"❌ RabbitMQ error: {e}", file=sys.stderr)

async def handle_tcp_client(reader, writer):
    buffer = b""
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            buffer += data
            if b"[s][save_image][e]" in buffer:
                buffer = buffer.replace(b"[s][save_image][e]", b"")
                # Запускаем обработку фоном, чтобы не блокировать TCP-поток
                asyncio.create_task(process_image())
    except Exception as e:
        print(f"TCP connection issue: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    ws_clients.add(ws)
    try:
        async for msg in ws:
            pass
    finally:
        ws_clients.remove(ws)
    return ws

async def start_ws_server():
    app = web.Application()
    app.add_routes([web.get('/ws', ws_handler)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', WS_PORT)
    await site.start()

async def main():
    await start_ws_server()
    tcp_server = await asyncio.start_server(handle_tcp_client, '0.0.0.0', TCP_PORT)
    print(f"🚀 Checker started. Listening TCP on {TCP_PORT}, WS on {WS_PORT}")
    async with tcp_server:
        await tcp_server.serve_forever()

if __name__ == '__main__':
    asyncio.run(main())