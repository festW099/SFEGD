FROM python:3.10-slim

WORKDIR /app

# Создаем пользователя для приложения
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py start.py run.py ./

# Переключаемся на непривилегированного пользователя
USER appuser

CMD ["python", "main.py"]