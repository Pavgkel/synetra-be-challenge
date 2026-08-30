import socket
import sys
import time


class TcpServer:
  def __init__(self, host: str, port: int) -> None:
    self._host = host
    self._port = port

  def _handle_connection(self, sock: socket.socket) -> None:
    while True:
      conn, addr = sock.accept()
      print(f"Connected by {addr}")

      try:
        time.sleep(10)
        for i in range(1, 6):
          message = "[s][save_image][e]"
          conn.sendall(message.encode("utf-8"))
          print(f"Send: {message.strip()}")
          time.sleep(5)

        while True:
          data = conn.recv(1024)
          if not data:
            print(f"Client {addr} disconnected...")
            break
          print(f"Got message from client {addr}: {data.decode('utf-8')}")
      except ConnectionResetError:
        print(f"Client {addr} closed connection.")
      finally:
        conn.close()

  def run(self) -> None:
    try:
      with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((self._host, self._port))
        sock.listen()

        print(f"Server is listening on {self._host}:{self._port}")

        self._handle_connection(sock)
    except Exception as e:
      print(f"Error occurred: {e}")
      sys.exit(1)
