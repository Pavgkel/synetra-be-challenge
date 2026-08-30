import os
import sys

from dotenv import load_dotenv
from worker.worker import Worker


def main():
  try:
    load_dotenv()
    TCP_HOST = os.getenv("TCP_HOST", "127.0.0.1")
    TCP_PORT = int(os.getenv("TCP_PORT", 4096))
    WS_HOST = os.getenv("WS_HOST", "127.0.0.1")
    WS_PORT = int(os.getenv("WS_PORT", 1234))
    worker = Worker(tcp_host=TCP_HOST, tcp_port=TCP_PORT, ws_host=WS_HOST, ws_port=WS_PORT)
    worker.run()
  except Exception as e:
    print(f"Error occurred: {e}")
    sys.exit(1)


if __name__ == "__main__":
  main()
