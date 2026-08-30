FROM python:3.11.9-slim

WORKDIR /app

COPY services/checker/requirements.txt ./

RUN pip install -r requirements.txt

COPY ./.env ./

COPY services/checker/app/ ./

CMD [ "python", "-u", "main.py" ]