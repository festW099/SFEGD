<div align="center">

[![Docker](https://img.shields.io/badge/Docker-20.10+-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)
[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Telegram](https://img.shields.io/badge/Telegram-Bot-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots)
[![Sber](https://img.shields.io/badge/Sber-GigaChat-2E9B4C?style=for-the-badge&logo=sber&logoColor=white)](https://developers.sber.ru)

**Простой и эффективный способ развернуть Telegram-бота с интеграцией GigaChat API**

</div>

---

## 🔑 Предварительные требования

Перед началом работы убедитесь, что у вас установлены:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- Git (для клонирования репозитория)

## 🎫 Получение ключей доступа

### 1️⃣ **BOT_TOKEN** (Telegram)
1. Найдите в Telegram бота [@BotFather](https://t.me/BotFather)
2. Отправьте команду `/newbot`
3. Следуйте инструкциям для создания нового бота
4. Скопируйте полученный токен (формат: `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`)

### 2️⃣ **CLIENT_ID** и **AUTH_KEY** (GigaChat)
1. Перейдите на [портал разработчиков Sber](https://developers.sber.ru/)
2. В разделе "Мои приложения" создайте новое приложение
3. Выберите тип доступа к GigaChat API
4. В настройках созданного приложения найдите:
   - **Client ID** (идентификатор приложения)
   - **Authorization Key** (секретный ключ)

> 💡 **Если что:** Можете использовать дефолтные данные из .env.


### Шаг 1: Клонирование репозитория
```bash
git clone https://github.com/festW099/SFEGD.git
cd SFEGD
Шаг 2: Подготовка данных
Создайте структуру для JSON-файлов:

Поместите ваш файл с данными в папку json

Шаг 3: Запуск контейнера
docker-compose up -d
docker-compose logs -f

Что произойдет при первом запуске:

✅ Автоматически создастся база данных

✅ Данные из JSON-файла загрузятся в БД

✅ Бот запустится и начнет отвечать на сообщения

✅ Все зависимости установятся автоматически

Проверка работы бота
Откройте Telegram

Найдите бота по имени

Отправьте команду /start

Начните диалог!

📁 Структура проекта
├── 📄 docker-compose.yml      # Конфигурация Docker
├── 📄 .env                    # Переменные окружения
├── 📁 json/                   # Директория с данными
│   └── 📄 data.json           # Ваш файл с данными
├── 📄 main.py                 # Исходный код бота
├── 📄 requirements.txt        # зависимости
├── 📄 README.md               # Этот файл
├── 📁 data                    # папка с db
