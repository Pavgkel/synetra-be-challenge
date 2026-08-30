import asyncio
import json
import sys

import websockets


class WebSocketClient:
  def __init__(self, host: str, port: int) -> None:
    self._uri = f"ws://{host}:{port}"

  async def _read_ws(self, websocket) -> None:
    while True:
      try:
        message = await websocket.recv()
        try:
          data = json.loads(message)
          print(f"Got a JSON message: {data}")
        except json.JSONDecodeError:
          print(f"Got not a JSON message: {message}")
      except websockets.exceptions.ConnectionClosedError as e:
        print(f"Connection closed: {e}")
        break
      except Exception as e:
        print(f"Error while reading from WebSocket: {e}")
        break

  async def _run(self) -> None:
    while True:
      try:
        print(f"Attempting to connect to WebSocket server: {self._uri}")
        async with websockets.connect(self._uri) as websocket:
          print(f"Connected to WebSocket server: {self._uri}")
          await self._read_ws(websocket)
      except (websockets.exceptions.InvalidURI, websockets.exceptions.InvalidHandshake) as e:
        print(f"Invalid WebSocket URI or handshake failed: {e}")
        sys.exit(1)
      except Exception as e:
        print(f"Error while connecting to WebSocket server: {e}")
        print("Reattempting connection in 15 seconds...")
        await asyncio.sleep(15)

  def run(self) -> None:
    asyncio.run(self._run())
