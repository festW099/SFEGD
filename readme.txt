Подготовьте данные:

BOT_TOKEN – получите у @BotFather

CLIENT_ID и AUTH_KEY – получите на портале Sber после создания приложения GigaChat API.

Подготовьте JSON-файл с данными

Запустите контейнер

bash
docker-compose up -d
При первом запуске автоматически создастся база данных и заполнится данными из JSON. Бот начнёт отвечать на сообщения.

Просмотр логов

bash
docker-compose logs -f
Остановка

bash
docker-compose down