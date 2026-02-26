import asyncio
import logging
import sqlite3
import json
import re
from datetime import datetime, timedelta
import requests
import aiohttp
import ssl
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage

DB_PATH = "/data/videos.db"


BOT_TOKEN = "8649941861:AAHrTD-0zVNIDmofagP6EOySUJVMBJC2L6M"
CLIENT_ID = "c91391d1-4218-4b77-826d-cd119ecb72e7"
AUTH_KEY = "MDE5YzhlOGQtNGQwNS03ZmE2LWJhNmYtZDNjYWM3YjU2NTJiOjU3MzY5ODMzLWM5Y2MtNGU2Mi05YTlhLWNlYWY0OWI4ZWY2MQ=="

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

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
Ты помощник аналитической системы.

Преобразуй запрос пользователя в JSON строго в формате:

{{
  "type": "total_videos | creator_videos_count | creator_videos_above_threshold | videos_with_threshold | total_views_growth | videos_with_new_views | total_likes | videos_published_in_period | total_views_published_in_period | total_likes_growth | total_comments_growth_first_hours",
  "creator_id": null или строка,
  "date_from": null или YYYY-MM-DD,
  "date_to": null или YYYY-MM-DD,
  "threshold": null или число,
  "hours": null или число
}}

ВАЖНЫЕ ПРАВИЛА:
1. Если пользователь спрашивает про КОНКРЕТНУЮ ДАТУ (например, "28 ноября 2025", "сегодня", "вчера"), то:
   - date_from = YYYY-MM-DD (начало дня)
   - date_to = YYYY-MM-DD (конец дня)
   
2. Если пользователь спрашивает про ПЕРИОД (например, "с 1 по 5 ноября", "за последнюю неделю"), то:
   - date_from = начало периода
   - date_to = конец периода

3. Если в запросе упоминается конкретный креатор (по id или username), обязательно укажи creator_id

4. Если запрос про "сколько видео у креатора набрали больше N просмотров" - используй тип "creator_videos_above_threshold"

5. Если запрос про "сколько видео набрали больше N просмотров" без указания креатора - используй тип "videos_with_threshold"

Верни ТОЛЬКО JSON.
Без текста.
Без комментариев.

Примеры:
- "На сколько просмотров в сумме выросли все видео 28 ноября 2025 года?" -> {{"type": "total_views_growth", "date_from": "2025-11-28", "date_to": "2025-11-28", "creator_id": null, "threshold": null, "hours": null}}
- "Сколько новых видео опубликовал креатор 123 за последнюю неделю?" -> {{"type": "creator_videos_count", "creator_id": "123", "date_from": "2026-02-19", "date_to": "2026-02-26", "threshold": null, "hours": null}}

Запрос:
{user_query}
"""

async def execute_query(data: dict):

    conn = get_db_connection()
    cursor = conn.cursor()
    result = 0

    type_ = data.get("type")
    creator_id = data.get("creator_id")
    date_from = data.get("date_from")
    date_to = data.get("date_to")
    threshold = data.get("threshold")
    hours = data.get("hours", 3)

    def format_date_range(date_from, date_to):
        if not date_from or not date_to:
            return date_from, date_to
        
        if len(date_from) == 10:
            date_from = date_from + " 00:00:00"
        if len(date_to) == 10:
            date_to = date_to + " 23:59:59"
        
        return date_from, date_to

    try:
        if type_ == "total_videos":
            cursor.execute("SELECT COUNT(*) as count FROM videos")
            result = cursor.fetchone()["count"]

        elif type_ == "total_likes":
            cursor.execute("SELECT SUM(likes_count) as total FROM videos")
            r = cursor.fetchone()["total"]
            result = r if r else 0

        elif type_ == "videos_with_threshold":
            cursor.execute(
                "SELECT COUNT(*) as count FROM videos WHERE views_count > ?",
                (threshold,)
            )
            result = cursor.fetchone()["count"]

        elif type_ == "creator_videos_above_threshold":
            if not creator_id:
                result = 0
            else:
                cursor.execute(
                    "SELECT COUNT(*) as count FROM videos WHERE creator_id=? AND views_count > ?",
                    (creator_id, threshold)
                )
                result = cursor.fetchone()["count"]

        elif type_ == "creator_videos_count":
            if not creator_id:
                result = 0
            else:
                query = "SELECT COUNT(*) as count FROM videos WHERE creator_id=?"
                params = [creator_id]
                
                if date_from and date_to:
                    date_from, date_to = format_date_range(date_from, date_to)
                    query += " AND datetime(video_created_at) BETWEEN datetime(?) AND datetime(?)"
                    params.extend([date_from, date_to])
                
                cursor.execute(query, params)
                result = cursor.fetchone()["count"]

        elif type_ == "videos_published_in_period":
            if not date_from or not date_to:
                result = 0
            else:
                date_from, date_to = format_date_range(date_from, date_to)
                cursor.execute(
                    """
                    SELECT COUNT(*) as count FROM videos
                    WHERE datetime(video_created_at) BETWEEN datetime(?) AND datetime(?)
                    """,
                    (date_from, date_to)
                )
                result = cursor.fetchone()["count"]

        elif type_ == "total_views_published_in_period":
            if not date_from or not date_to:
                result = 0
            else:
                date_from, date_to = format_date_range(date_from, date_to)
                cursor.execute(
                    """
                    SELECT SUM(views_count) as total FROM videos
                    WHERE datetime(video_created_at) BETWEEN datetime(?) AND datetime(?)
                    """,
                    (date_from, date_to)
                )
                r = cursor.fetchone()["total"]
                result = r if r else 0

        elif type_ == "total_views_growth":
            if not date_from or not date_to:
                result = 0
            else:
                date_from, date_to = format_date_range(date_from, date_to)
                cursor.execute(
                    """
                    SELECT SUM(delta_views_count) as total FROM snapshots
                    WHERE datetime(created_at) BETWEEN datetime(?) AND datetime(?)
                    """,
                    (date_from, date_to)
                )
                r = cursor.fetchone()["total"]
                result = r if r else 0

        elif type_ == "total_likes_growth":
            if not date_from or not date_to:
                result = 0
            else:
                date_from, date_to = format_date_range(date_from, date_to)
                cursor.execute(
                    """
                    SELECT SUM(delta_likes_count) as total FROM snapshots
                    WHERE datetime(created_at) BETWEEN datetime(?) AND datetime(?)
                    """,
                    (date_from, date_to)
                )
                r = cursor.fetchone()["total"]
                result = r if r else 0

        elif type_ == "videos_with_new_views":
            if not date_from or not date_to:
                result = 0
            else:
                date_from, date_to = format_date_range(date_from, date_to)
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT video_id) as count FROM snapshots
                    WHERE delta_views_count > 0
                    AND datetime(created_at) BETWEEN datetime(?) AND datetime(?)
                    """,
                    (date_from, date_to)
                )
                result = cursor.fetchone()["count"]

        elif type_ == "total_comments_growth_first_hours":
            cursor.execute("SELECT id, video_created_at FROM videos")
            videos = cursor.fetchall()

            total = 0
            for video in videos:
                created = datetime.fromisoformat(video["video_created_at"])
                end = created + timedelta(hours=hours)

                cursor.execute(
                    """
                    SELECT SUM(delta_comments_count) as total FROM snapshots
                    WHERE video_id=?
                    AND datetime(created_at) BETWEEN datetime(?) AND datetime(?)
                    """,
                    (video["id"], created.isoformat(), end.isoformat())
                )
                r = cursor.fetchone()["total"]
                if r:
                    total += r

            result = total

    except Exception as e:
        logging.error(f"SQL Error: {e}")
        result = 0
    finally:
        conn.close()
    
    return result

@dp.message()
async def handle(message: types.Message):
    try:
        user_query = message.text
        
        logging.info(f"User query: {user_query}")

        ai_response = await gigachat.send_message(build_prompt(user_query))
        logging.info(f"AI RAW: {ai_response}")

        ai_response = re.sub(r'```json\n|\n```|```', '', ai_response).strip()
        
        data = json.loads(ai_response)
        logging.info(f"Parsed data: {data}")

        answer = await execute_query(data)
        logging.info(f"Query result: {answer}")

        await message.answer(str(answer))

    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}, AI response: {ai_response}")
        await message.answer("0")
    except Exception as e:
        logging.error(f"{e}")
        await message.answer("0")


async def main():
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    gigachat.get_auth_token()
    logging.info("ЗАПУСК")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())