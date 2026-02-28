import asyncio
import logging
import sqlite3
import json
import re
import os
from datetime import datetime
import requests
import aiohttp
import ssl
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage

# ------------------- НАСТРОЙКИ -------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "videos.db")

BOT_TOKEN = "8652912096:AAHv7rizsCjkC1GLA2fjdjLjtLGeAtjqTkw"
CLIENT_ID = "c91391d1-4218-4b77-826d-cd119ecb72e7"
AUTH_KEY = "MDE5YzhlOGQtNGQwNS03ZmE2LWJhNmYtZDNjYWM3YjU2NTJiOjU3MzY5ODMzLWM5Y2MtNGU2Mi05YTlhLWNlYWY0OWI4ZWY2MQ=="

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ------------------- БАЗА ДАННЫХ -------------------

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ------------------- GIGACHAT API -------------------

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

# ------------------- ПРОМПТ -------------------

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
    video_created_at TEXT
)

snapshots (
    id TEXT PRIMARY KEY,
    video_id TEXT,
    delta_views_count INTEGER,
    delta_likes_count INTEGER,
    delta_comments_count INTEGER,
    created_at TEXT
)

ПРАВИЛА:

1. Генерируй ТОЛЬКО SQL.
2. Только SELECT запросы.
3. Никаких комментариев в ответе.
4. Никаких объяснений.
5. Без ``` и другого форматирования.
6. Используй datetime() для работы с датами.
7. Если нужно получить одно число (сумму, количество) - используй агрегатные функции.

Примеры правильных запросов:

Вопрос: "Сколько всего просмотров у всех видео?"
Ответ: SELECT SUM(views_count) FROM videos;

Вопрос: "На сколько просмотров выросли все видео 28 ноября 2025?"
Ответ: SELECT SUM(delta_views_count) FROM snapshots WHERE datetime(created_at) BETWEEN datetime('2025-11-28 00:00:00') AND datetime('2025-11-28 23:59:59');

Вопрос: "Сколько лайков у видео за всё время?"
Ответ: SELECT SUM(likes_count) FROM videos;

Вопрос: "Какой суммарный прирост комментариев получили все видео за первые 3 часа после публикации каждого из них?"
Ответ: SELECT SUM(likes_count) FROM videos;

напоминаю, что в snapshots video_created_at не существует

Запрос пользователя:
{user_query}
"""

# ------------------- ВЫПОЛНЕНИЕ SQL -------------------

async def execute_ai_sql(sql_query: str):
    # Защита
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "PRAGMA"]
    if any(word in sql_query.upper() for word in forbidden):
        logging.error(f"Опасный SQL: {sql_query}")
        return "0"

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(sql_query)
        result = cursor.fetchone()
        
        # Если результат - кортеж с одним значением
        if result and len(result) == 1:
            return str(result[0] if result[0] is not None else 0)
        
        # Если результат - несколько значений или пустой
        elif result:
            return str(result[0] if result[0] is not None else 0)
        else:
            return "0"

    except Exception as e:
        logging.error(f"SQL Error: {e}")
        return "0"

    finally:
        conn.close()

# ------------------- ОБРАБОТКА СООБЩЕНИЙ -------------------

@dp.message()
async def handle(message: types.Message):
    try:
        user_query = message.text
        logging.info(f"User query: {user_query}")

        # Получаем SQL от GigaChat
        ai_response = await gigachat.send_message(build_prompt(user_query))
        logging.info(f"AI RAW SQL: {ai_response}")

        # Очистка SQL от markdown
        sql_query = re.sub(r'```sql|```', '', ai_response).strip()
        
        # Если после очистки пусто, берем оригинал
        if not sql_query:
            sql_query = ai_response.strip()

        logging.info(f"Cleaned SQL: {sql_query}")

        # Выполняем SQL
        result = await execute_ai_sql(sql_query)

        # Отправляем результат
        await message.answer(result)

    except Exception as e:
        logging.error(f"Error: {e}")
        await message.answer("Произошла ошибка при обработке запроса")

# ------------------- MAIN -------------------

async def main():
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Получаем токен при запуске
    gigachat.get_auth_token()
    logging.info("БОТ ЗАПУЩЕН")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())