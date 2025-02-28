import logging
import os
import atexit
import uuid
import json

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from confluent_kafka import Producer, Consumer

import threading

DB_ERROR_STR = "DB error"


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)

def create_kafka_producer():
    conf = {'bootstrap.servers': "kafka:9092"}
    producer = Producer(**conf)
    return producer

def create_kafka_consumer(topic):
    conf = {
        'bootstrap.servers': "kafka:9092",
        'group.id': "payments",
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(**conf)
    consumer.subscribe([topic])
    return consumer

producer = create_kafka_producer()

class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

def send_to_kafka(topic, data):
    producer.produce(topic, data)
    producer.flush()

@app.route('/kafka_demo', methods=['POST'])
def demo_kafka():
    """ Demo kafka """
    message = {
        'message': 'Hello',
        'from': 'payment',
    }
    send_to_kafka('demo_topic1', json.dumps(message))
    return jsonify({'status': 'Message sent'}), 200

def consume_messages(consumer):
    while True:
        message = consumer.poll(0.1)
        if message is None:
            continue
        if message.error():
            app.logger.info(f"Consumer error: {message.error()}")
            continue
        app.logger.info(f"Received message: {message.value().decode('utf-8')}")

def start_consumer_thread(topic):
    consumer = create_kafka_consumer(topic)
    thread = threading.Thread(target=consume_messages, args=(consumer,))
    thread.daemon = True
    thread.start()

if __name__ == '__main__':
    start_consumer_thread('demo_topic1')
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    start_consumer_thread('demo_topic1')
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
