FROM python:3.12-slim

WORKDIR /home/flask-app

RUN apt-get update && apt-get install -y \
    gcc \
    libc-dev \
    librdkafka-dev


COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000