# syntax=docker/dockerfile:1

FROM python:3.9-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

ENV MOEX_HOST="http://10.0.0.38"

ENV NEWS_HOST="http://10.0.0.80"

ENV STOCK_HOST="http://localhost:8080/db/stock"

ENV STOCK_DIR="/stock/"

CMD ["python3", "main.py"]
