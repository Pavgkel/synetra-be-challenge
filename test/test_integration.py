import asyncio
import json
import pytest
import aiohttp

TCP_PORT = 4096
WS_URL = "http://localhost:1234/ws"
API_URL = "http://localhost:8000/api/records"

@pytest.mark.asyncio
async def test_e2e_pipeline():
    # 1. Подключаемся к WebSocket чеккера в роли Фронтенда
    session = aiohttp.ClientSession()
    try:
        ws = await session.ws_connect(WS_URL, timeout=5.0)
    except Exception as e:
        await session.close()
        pytest.fail(f"Не удалось подключиться к WebSocket чеккера: {e}")

    # 2. Имитируем сигнал от PLC через сырое TCP-подключение
    try:
        reader, writer = await asyncio.open_connection('localhost', TCP_PORT)
        writer.write(b"[s][save_image][e]")
        await writer.drain()
        
        # Ждем ответа OK от нашего TCP-сервера
        response = await reader.read(1024)
        assert b"OK" in response
        
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        await ws.close()
        await session.close()
        pytest.fail(f"Ошибка отправки TCP команды PLC: {e}")

    # 3. Проверяем, что событие мгновенно транслировалось в WebSocket
    try:
        ws_msg = await ws.receive_str(timeout=5.0)
        ws_data = json.loads(ws_msg)
        assert "image_url" in ws_data
        assert "width" in ws_data
        assert "height" in ws_data
    except asyncio.TimeoutError:
        pytest.fail("WebSocket не получил событие от чеккера в течение 5 секунд")
    finally:
        await ws.close()
        await session.close()

    # 4. Проверяем API: запись обязана успешно закрепиться в SQLite
    async with aiohttp.ClientSession() as api_session:
        async with api_session.get(API_URL) as resp:
            assert resp.status == 200
            records = await resp.json()
            assert len(records) > 0
            # Последняя запись в базе должна совпадать с тем, что улетело в вебсокет
            assert records[-1]["image_url"] == ws_data["image_url"]
