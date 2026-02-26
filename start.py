import os
import sys
import sqlite3
import json
from datetime import datetime
from pathlib import Path
import re

CONFIG_FILE = "bot_config.json"

def print_header():
    print("=" * 60)
    print("      НАСТРОЙКА БОТА ДЛЯ АНАЛИЗА ВИДЕО")
    print("=" * 60)

def get_json_file_path():
    print("\n--- ЗАГРУЗКА ДАННЫХ ИЗ JSON ---")
    
    while True:
        json_path = input("Введите путь к JSON файлу с данными о видео: ").strip()
        
        if not json_path:
            print("✗ Путь к JSON файлу не может быть пустым")
            continue
        
        if not os.path.exists(json_path):
            print(f"✗ Файл не найден: {json_path}")
            continue
        
        if not json_path.endswith('.json'):
            print("✗ Файл должен иметь расширение .json")
            continue
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                if isinstance(data, dict) and 'videos' in data and isinstance(data['videos'], list):
                    print(f"✓ Найдено {len(data['videos'])} записей в JSON файле")
                    return json_path, data['videos']
                elif isinstance(data, list):
                    print(f"✓ Найдено {len(data)} записей в JSON файле (прямой массив)")
                    return json_path, data
                else:
                    print("✗ JSON файл должен содержать объект с ключом 'videos' или прямой массив")
                    continue
                    
        except json.JSONDecodeError as e:
            print(f"✗ Ошибка при чтении JSON: {e}")
        except Exception as e:
            print(f"✗ Ошибка при открытии файла: {e}")

def get_bot_token():
    print("\n--- НАСТРОЙКА TELEGRAM БОТА ---")
    default_token = "8649941861:AAHrTD-0zVNIDmofagP6EOySUJVMBJC2L6M"
    print("Создать своего бота и получить токен можно по ссылке: https://telegram.me/s/BotFther")
    token = input(f"Введите BOT_TOKEN (Enter для использования токена по умолчанию): ").strip()
    
    if not token:
        token = default_token
        print(f"✓ Используется токен по умолчанию")
    else:
        print(f"✓ Токен установлен")
    
    return token

def get_client_id():
    print("\n--- НАСТРОЙКА GIGACHAT API ---")
    default_client_id = "c91391d1-4218-4b77-826d-cd119ecb72e7"
    print("Получить свой id можно по ссылке https://developers.sber.ru/portal/products/gigachat-api")
    client_id = input(f"Введите CLIENT_ID (Enter для использования ID по умолчанию): ").strip()
    
    if not client_id:
        client_id = default_client_id
        print(f"✓ Используется Client ID по умолчанию")
    else:
        print(f"✓ Client ID установлен")
    
    return client_id

def get_auth_key():
    print("\n--- НАСТРОЙКА AUTH KEY ---")
    default_auth_key = "MDE5YzhlOGQtNGQwNS03ZmE2LWJhNmYtZDNjYWM3YjU2NTJiOjU3MzY5ODMzLWM5Y2MtNGU2Mi05YTlhLWNlYWY0OWI4ZWY2MQ=="
    print("Получить свой ключ можно по ссылке https://developers.sber.ru/portal/products/gigachat-api")
    auth_key = input(f"Введите AUTH_KEY (Enter для использования ключа по умолчанию): ").strip()
    
    if not auth_key:
        auth_key = default_auth_key
        print(f"✓ Используется Auth Key по умолчанию")
    else:
        print(f"✓ Auth Key установлен")
    
    return auth_key

def create_database_schema(conn):
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            id TEXT PRIMARY KEY,
            creator_id TEXT NOT NULL,
            video_created_at TIMESTAMP NOT NULL,
            views_count INTEGER DEFAULT 0,
            likes_count INTEGER DEFAULT 0,
            comments_count INTEGER DEFAULT 0,
            reports_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            views_count INTEGER DEFAULT 0,
            likes_count INTEGER DEFAULT 0,
            comments_count INTEGER DEFAULT 0,
            reports_count INTEGER DEFAULT 0,
            delta_views_count INTEGER DEFAULT 0,
            delta_likes_count INTEGER DEFAULT 0,
            delta_comments_count INTEGER DEFAULT 0,
            delta_reports_count INTEGER DEFAULT 0,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (video_id) REFERENCES videos (id) ON DELETE CASCADE
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_videos_creator_id ON videos(creator_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(video_created_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_snapshots_video_id ON snapshots(video_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON snapshots(created_at)')
    
    conn.commit()

def process_json_data(conn, json_data):
    cursor = conn.cursor()
    
    videos_count = 0
    snapshots_count = 0
    skipped_videos = 0
    skipped_snapshots = 0
    
    for video in json_data:
        try:
            if 'id' not in video:
                print(f"⚠ Пропуск видео: отсутствует поле 'id'")
                skipped_videos += 1
                continue
            
            if 'creator_id' not in video:
                print(f"⚠ Пропуск видео {video.get('id', 'unknown')}: отсутствует поле 'creator_id'")
                skipped_videos += 1
                continue
            
            if 'video_created_at' not in video:
                print(f"⚠ Пропуск видео {video['id']}: отсутствует поле 'video_created_at'")
                skipped_videos += 1
                continue
            
            cursor.execute('''
                INSERT OR REPLACE INTO videos 
                (id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                video['id'],
                video['creator_id'],
                video['video_created_at'],
                video.get('views_count', 0),
                video.get('likes_count', 0),
                video.get('comments_count', 0),
                video.get('reports_count', 0)
            ))
            videos_count += 1
            
            if 'snapshots' in video and isinstance(video['snapshots'], list):
                for snapshot in video['snapshots']:
                    try:
                        if 'created_at' not in snapshot:
                            print(f"⚠ Пропуск снапшота для видео {video['id']}: отсутствует поле 'created_at'")
                            skipped_snapshots += 1
                            continue
                        
                        cursor.execute('''
                            INSERT INTO snapshots 
                            (video_id, views_count, likes_count, comments_count, reports_count, 
                             delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            video['id'],
                            snapshot.get('views_count', 0),
                            snapshot.get('likes_count', 0),
                            snapshot.get('comments_count', 0),
                            snapshot.get('reports_count', 0),
                            snapshot.get('delta_views_count', 0),
                            snapshot.get('delta_likes_count', 0),
                            snapshot.get('delta_comments_count', 0),
                            snapshot.get('delta_reports_count', 0),
                            snapshot['created_at']
                        ))
                        snapshots_count += 1
                    except Exception as e:
                        print(f"✗ Ошибка при обработке снапшота для видео {video['id']}: {e}")
                        skipped_snapshots += 1
                        continue
            
            conn.commit()
            
        except Exception as e:
            print(f"✗ Ошибка при обработке видео {video.get('id', 'unknown')}: {e}")
            skipped_videos += 1
            conn.rollback()
    return videos_count, snapshots_count

def save_config(config):
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=4)
        print(f"✓ Конфигурация сохранена в {CONFIG_FILE}")
        return True
    except Exception as e:
        print(f"✗ Ошибка при сохранении конфигурации: {e}")
        return False

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
            print(f"✓ Загружена конфигурация из {CONFIG_FILE}")
            return config
        except Exception as e:
            print(f"✗ Ошибка при загрузке конфигурации: {e}")
    return None

def update_main_py(config):
    main_py_path = "main.py"
    
    if not os.path.exists(main_py_path):
        print(f"✗ Файл {main_py_path} не найден. Пропускаем обновление.")
        return False
    
    try:
        with open(main_py_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        content = re.sub(r'BOT_TOKEN = ".*?"', f'BOT_TOKEN = "{config["bot_token"]}"', content)
        content = re.sub(r'DB_PATH = ".*?"', f'DB_PATH = "{config["db_path"]}"', content)
        content = re.sub(r'CLIENT_ID = ".*?"', f'CLIENT_ID = "{config["client_id"]}"', content)
        content = re.sub(r'AUTH_KEY = ".*?"', f'AUTH_KEY = "{config["auth_key"]}"', content)
        
        with open(main_py_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✓ Файл {main_py_path} успешно обновлен")
        return True
        
    except Exception as e:
        print(f"✗ Ошибка при обновлении {main_py_path}: {e}")
        return False

def main():
    print_header()
    
    db_path = "data/videos.db"
    print(f"\n✓ База данных будет создана по пути: {db_path}")
    
    json_path, json_data = get_json_file_path()
    
    existing_config = load_config()
    if existing_config:
        print("\nНайдена существующая конфигурация:")
        print(f"  - BOT_TOKEN: {'установлен' if existing_config.get('bot_token') else 'не указан'}")
        print(f"  - CLIENT_ID: {'установлен' if existing_config.get('client_id') else 'не указан'}")
        print(f"  - AUTH_KEY: {'установлен' if existing_config.get('auth_key') else 'не указан'}")
        
        use_existing = input("\nИспользовать существующие токены? (д/н): ").strip().lower()
        if use_existing in ['д', 'да', 'y', 'yes', '']:
            bot_token = existing_config['bot_token']
            client_id = existing_config['client_id']
            auth_key = existing_config['auth_key']
            print("✓ Используются существующие токены")
        else:
            bot_token = get_bot_token()
            client_id = get_client_id()
            auth_key = get_auth_key()
    else:
        bot_token = get_bot_token()
        client_id = get_client_id()
        auth_key = get_auth_key()
    
    if os.path.exists(db_path):
        print(f"\n⚠ База данных уже существует по пути: {db_path}")
        replace_db = input("Хотите заменить существующую базу данных новой? (д/н): ").strip().lower()
        
        if replace_db in ['д', 'да', 'y', 'yes']:
            backup_path = f"{db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            try:
                os.rename(db_path, backup_path)
                print(f"✓ Создана резервная копия: {backup_path}")
            except Exception as e:
                print(f"✗ Ошибка при создании резервной копии: {e}")
            
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
            
            conn = sqlite3.connect(db_path)
            create_database_schema(conn)
            videos_count, snapshots_count = process_json_data(conn, json_data)
            conn.close()
            
            print(f"\n✓ Создана новая база данных")
        else:
            print("✓ Существующая база данных сохранена")
            
            add_data = input("Хотите добавить данные из JSON к существующим? (д/н): ").strip().lower()
            if add_data in ['д', 'да', 'y', 'yes']:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='videos'")
                if not cursor.fetchone():
                    create_database_schema(conn)
                videos_count, snapshots_count = process_json_data(conn, json_data)
                conn.close()
                print(f"\n✓ Данные добавлены в существующую базу данных")
    else:
        print("\n--- СОЗДАНИЕ БАЗЫ ДАННЫХ ---")
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            print(f"✓ Создана директория: {db_dir}")
        
        conn = sqlite3.connect(db_path)
        create_database_schema(conn)
        videos_count, snapshots_count = process_json_data(conn, json_data)
        conn.close()
        
        print(f"✓ Создана новая база данных")
    
    config = {
        "db_path": db_path,
        "bot_token": bot_token,
        "client_id": client_id,
        "auth_key": auth_key,
        "json_source": json_path,
        "created_at": datetime.now().isoformat()
    }
    
    save_config(config)
    
    update_main_py(config)
    
    print("\n" + "=" * 60)
    print("      НАСТРОЙКА ЗАВЕРШЕНА УСПЕШНО!")
    print("=" * 60)
    print(f"\nДля запуска бота выполните:")
    print(f"  python main.py")
    print("\nИспользуемые параметры:")
    print(f"  - База данных: {db_path}")
    print(f"  - JSON источник: {json_path}")
    print(f"  - BOT_TOKEN: {'*' * 10}{bot_token[-10:] if len(bot_token) > 10 else 'установлен'}")
    print(f"  - CLIENT_ID: {'*' * 10}{client_id[-10:] if len(client_id) > 10 else 'установлен'}")
    print(f"  - AUTH_KEY: {'*' * 10}{auth_key[-10:] if len(auth_key) > 10 else 'установлен'}")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ Настройка прервана пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Критическая ошибка: {e}")
        sys.exit(1)