FROM python:3.12-slim

WORKDIR /home/flask-app

RUN apt-get update && apt-get install -y \
    gcc \
    libc-dev \
    librdkafka-dev \
    dos2unix \
    netcat-openbsd

COPY wait-for-it.sh .
RUN dos2unix wait-for-it.sh && chmod +x wait-for-it.sh

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000