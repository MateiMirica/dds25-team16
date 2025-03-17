import logging
import os
import atexit
import uuid

import redis
import uvicorn

from msgspec import msgpack, Struct
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from faststream.kafka.fastapi import KafkaRouter


from payment_worker import PaymentWorker, UserValue

DB_ERROR_STR = "DB error"


app = FastAPI(title="payment-service")
router = KafkaRouter("kafka:9092")
app.include_router(router)

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)

logging.basicConfig(level=logging.CRITICAL+1, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
paymentWorker = PaymentWorker(logger, db, router)

@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    return {'user_id': key}


@app.post('/batch_init/{n}/{starting_money}')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    return {"msg": "Batch init for users successful"}


@app.get('/find_user/{user_id}')
def find_user(user_id: str):
    user_entry: UserValue = None
    try:
        user_entry = paymentWorker.get_user_from_db(user_id)
    except:
        raise HTTPException(400, DB_ERROR_STR)
    if user_entry == None:
        raise HTTPException(400, "No such user")

    return {
        "user_id": user_id,
        "credit": user_entry.credit
    }
    


@app.post('/add_funds/{user_id}/{amount}')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = None
    try:
        user_entry = paymentWorker.get_user_from_db(user_id)
    except:
        raise HTTPException(400, DB_ERROR_STR)
    if user_entry == None:
        raise HTTPException(400, "No such user")
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status_code=200)


@app.post('/pay/{user_id}/{amount}')
def remove_credit(user_id: str, amount: int):
    logging.debug(f"Removing {amount} credit from user: {user_id}")
    try:
        user_entry = paymentWorker.get_user_from_db(user_id)
    except:
        raise HTTPException(400, DB_ERROR_STR)
    if user_entry == None:
        raise HTTPException(400, "No such user")
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        raise HTTPException(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status_code=200)



if __name__ == '__main__':
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
else:
    logger.info("Starting Payment Service with uvicorn")
