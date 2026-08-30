FROM python:3.11-slim
WORKDIR /app

# Копируем requirements из конкретной папки сервиса
COPY ./services/api/requirements.txt ./ 

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Запуск скрипта (путь внутри WORKDIR)
CMD ["python", "services/api/main.py"] 