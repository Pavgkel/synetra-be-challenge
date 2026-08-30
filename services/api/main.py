import os
import sqlite3
from aiohttp import web

DB_PATH = os.getenv("DB_PATH", "/app/data/sqlite/excercise.db")

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                image_url TEXT NOT NULL,
                width INTEGER NOT NULL,
                height INTEGER NOT NULL
            )
        """)

async def create_record(request):
    try:
        data = await request.json()
        image_url = data['image_url']
        width = int(data['width'])
        height = int(data['height'])
    except (ValueError, KeyError, TypeError):
        return web.json_response({"error": "Invalid payload"}, status=400)

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO images (image_url, width, height) VALUES (?, ?, ?)",
            (image_url, width, height)
        )
        conn.commit()
        row_id = cursor.lastrowid

    return web.json_response({"id": row_id, "image_url": image_url, "width": width, "height": height}, status=201)

async def get_record(request):
    record_id = request.match_info.get('id')
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT id, image_url, width, height FROM images WHERE id = ?", (record_id,))
        row = cursor.fetchone()
    
    if not row:
        return web.json_response({"error": "Not found"}, status=404)
    return web.json_response(dict(row))

async def list_records(request):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT id, image_url, width, height FROM images")
        rows = cursor.fetchall()
    return web.json_response([dict(row) for row in rows])

app = web.Application()
app.add_routes([
    web.post('/api/records', create_record),
    web.get('/api/records/{id}', get_record),
    web.get('/api/records', list_records)
])

if __name__ == '__main__':
    init_db()
    print("🚀 API Service running on port 8000")
    web.run_app(app, host='0.0.0.0', port=8000)