import asyncio
import aiohttp

async def test():
    try:
        async with aiohttp.ClientSession() as s:
            print("Попытка подключения к WebSocket...")
            async with s.ws_connect('http://localhost:1234/ws') as ws:
                print('✅ WS Успешно подключен! Ожидание событий от PLC...')
                async for m in ws:
                    print('📡 WS Получено:', m.data)
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")

asyncio.run(test())
