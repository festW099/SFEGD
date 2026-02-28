import asyncio
import logging
import sqlite3
import json
import re
import os
import glob
import urllib3
from datetime import datetime
import requests
import aiohttp
import ssl
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage

BOT_TOKEN = "8652912096:AAHv7rizsCjkC1GLA2fjdjLjtLGeAtjqTkw"
CLIENT_ID = "c91391d1-4218-4b77-826d-cd119ecb72e7"
AUTH_KEY = "MDE5YzhlOGQtNGQwNS03ZmE2LWJhNmYtZDNjYWM3YjU2NTJiOjU3MzY5ODMzLWM5Y2MtNGU2Mi05YTlhLWNlYWY0OWI4ZWY2MQ=="

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(BASE_DIR, "json")
DB_PATH = os.path.join(BASE_DIR, "data", "videos.db")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

def find_json_file():
    
    if not os.path.exists(JSON_DIR):
        logging.error("Папка json не найдена")
        return None
    
    json_files = glob.glob(os.path.join(JSON_DIR, "*.json"))
    
    if not json_files:
        logging.error("В папке json нет JSON файлов")
        return None
    
    return json_files[0]

def create_database_from_json():
    if os.path.exists(DB_PATH):
        logging.info("База данных уже существует")
        return True
    
    logging.info("База данных не найдена. Создаем из JSON...")
    
    json_path = find_json_file()
    if not json_path:
        logging.error("Не удалось найти JSON файл для создания БД")
        return False
    
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS videos (
        id TEXT PRIMARY KEY,
        creator_id TEXT,
        views_count INTEGER,
        likes_count INTEGER,
        comments_count INTEGER,
        video_created_at TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS snapshots (
        id TEXT PRIMARY KEY,
        video_id TEXT,
        delta_views_count INTEGER,
        delta_likes_count INTEGER,
        delta_comments_count INTEGER,
        created_at TEXT,
        FOREIGN KEY (video_id) REFERENCES videos (id)
    )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_snapshots_video_id ON snapshots(video_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON snapshots(created_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(video_created_at)')
    
    conn.commit()
    
    try:
        with open(json_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        if isinstance(data, dict) and "videos" in data:
            videos_data = data["videos"]
        elif isinstance(data, list):
            videos_data = data
        else:
            logging.error("Неподдерживаемая структура JSON")
            conn.close()
            return False
        
        cursor.execute("DELETE FROM snapshots")
        cursor.execute("DELETE FROM videos")
        
        for video in videos_data:
            if not isinstance(video, dict):
                continue
                
            try:
                cursor.execute('''
                INSERT OR REPLACE INTO videos 
                (id, creator_id, views_count, likes_count, comments_count, video_created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    video.get('id', ''),
                    video.get('creator_id', ''),
                    video.get('views_count', 0),
                    video.get('likes_count', 0),
                    video.get('comments_count', 0),
                    video.get('video_created_at', '')
                ))
                
                for snapshot in video.get('snapshots', []):
                    cursor.execute('''
                    INSERT OR REPLACE INTO snapshots 
                    (id, video_id, delta_views_count, delta_likes_count, delta_comments_count, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        snapshot.get('id', ''),
                        video.get('id', ''),
                        snapshot.get('delta_views_count', 0),
                        snapshot.get('delta_likes_count', 0),
                        snapshot.get('delta_comments_count', 0),
                        snapshot.get('created_at', '')
                    ))
                    
            except Exception as e:
                logging.error(f"Ошибка при вставке видео: {e}")
                continue
        
        conn.commit()
        conn.close()
            
    except Exception as e:
        logging.error(f"Ошибка при чтении JSON: {e}")
        conn.close()
        return False

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

class GigaChatAPI:
    def __init__(self):
        self.auth_token = None

    def get_auth_token(self):
        url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'RqUID': CLIENT_ID,
            'Authorization': f'Basic {AUTH_KEY}'
        }

        payload = {'scope': 'GIGACHAT_API_PERS'}

        response = requests.post(url, headers=headers, data=payload, verify=False)

        if response.status_code == 200:
            self.auth_token = response.json().get("access_token")
            return self.auth_token
        return None

    async def send_message(self, prompt: str):
        if not self.auth_token:
            self.get_auth_token()

        url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.auth_token}'
        }

        data = {
            "model": "GigaChat",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
            "max_tokens": 500
        }

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
            async with session.post(url, headers=headers, json=data) as response:
                result = await response.json()
                return result["choices"][0]["message"]["content"]

gigachat = GigaChatAPI()

def build_prompt(user_query: str) -> str:
    return f"""
Ты генератор SQL-запросов для SQLite.

База данных содержит таблицы:

videos (
    id TEXT PRIMARY KEY,
    creator_id TEXT,
    views_count INTEGER,
    likes_count INTEGER,
    comments_count INTEGER,
    video_created_at TEXT  -- формат: YYYY-MM-DD HH:MM:SS
)

snapshots (
    id TEXT PRIMARY KEY,
    video_id TEXT,
    delta_views_count INTEGER,
    delta_likes_count INTEGER,
    delta_comments_count INTEGER,
    created_at TEXT        -- формат: YYYY-MM-DD HH:MM:SS
)

ВАЖНО: Все даты хранятся в формате 'YYYY-MM-DD HH:MM:SS'. 
Для корректного сравнения дат ВСЕГДА используй функцию datetime().

ПРАВИЛА:
1. Генерируй ТОЛЬКО SQL.
2. Только SELECT запросы.
3. Никаких комментариев в ответе.
4. Без ``` и другого форматирования.
5. Для сравнения дат всегда используй datetime(поле) BETWEEN datetime(начало) AND datetime(конец)

Примеры правильных запросов:

Вопрос: "Сколько всего просмотров у всех видео?"
Ответ: SELECT SUM(views_count) FROM videos;

Вопрос: "Какое суммарное количество просмотров набрали все видео, опубликованные в июне 2025 года?"
Ответ: SELECT SUM(views_count) FROM videos WHERE datetime(video_created_at) BETWEEN datetime('2025-06-01 00:00:00') AND datetime('2025-06-30 23:59:59');

Вопрос: "Какой суммарный прирост комментариев получили все видео за первые 3 часа после публикации каждого из них?"
Ответ: SELECT SUM(s.delta_comments_count) 
FROM snapshots s 
JOIN videos v ON s.video_id = v.id 
WHERE datetime(s.created_at) BETWEEN datetime(v.video_created_at) AND datetime(v.video_created_at, '+3 hours');

Вопрос: "На сколько просмотров выросли все видео 28 ноября 2025?"
Ответ: SELECT SUM(delta_views_count) FROM snapshots WHERE date(created_at) = '2025-11-28';

Запрос пользователя:
{user_query}
"""

async def execute_ai_sql(sql_query: str):
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "PRAGMA"]
    if any(word in sql_query.upper() for word in forbidden):
        logging.error(f"Опасный SQL: {sql_query}")
        return "0"

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(sql_query)
        result = cursor.fetchone()
        
        if result and len(result) == 1:
            return str(result[0] if result[0] is not None else 0)
        
        elif result:
            return str(result[0] if result[0] is not None else 0)
        else:
            return "0"

    except Exception as e:
        logging.error(f"SQL Error: {e}")
        return "0"

    finally:
        conn.close()

@dp.message()
async def handle(message: types.Message):
    try:
        user_query = message.text
        logging.info(f"User query: {user_query}")

        ai_response = await gigachat.send_message(build_prompt(user_query))
        logging.info(f"AI RAW SQL: {ai_response}")

        sql_query = re.sub(r'```sql|```', '', ai_response).strip()
        
        if not sql_query:
            sql_query = ai_response.strip()

        logging.info(f"Cleaned SQL: {sql_query}")

        result = await execute_ai_sql(sql_query)

        await message.answer(result)

    except Exception as e:
        logging.error(f"Error: {e}")
        await message.answer("Произошла ошибка при обработке запроса")

async def main():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    if not create_database_from_json():
        logging.error("Не удалось создать базу данных. Бот не может быть запущен.")
        return
    logging.info("Получение токена GigaChat...")
    gigachat.get_auth_token()
    
    logging.info("БОТ ЗАПУЩЕН")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())