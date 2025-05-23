import logging
import os
import atexit
import uuid

import redis
import uvicorn

from msgspec import msgpack
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from faststream.kafka.fastapi import KafkaRouter
from redis.sentinel import Sentinel

from payment_worker import PaymentWorker, UserValue

DB_ERROR_STR = "DB error"


app = FastAPI(title="payment-service")
router = KafkaRouter("kafka:9092", logger=None)
app.include_router(router)

sentinel_hosts = [
    (host.split(":")[0], int(host.split(":")[1]))
    for host in os.environ["REDIS_SENTINEL_HOSTS"].split(",")
]

sentinel = Sentinel(
    sentinel_hosts,
    socket_timeout=0.5,
    password=os.environ["REDIS_PASSWORD"]
)

db = sentinel.master_for(
    os.environ["REDIS_MASTER_NAME"],
    socket_timeout=0.5,
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ.get("REDIS_DB", 0))
)

add_funds_lua_script = db.register_script(
    """
    local userId = KEYS[1]
    local amount = tonumber(ARGV[1])

    local user_data = redis.call("GET", userId)
    if not user_data then
        return {"USER_NOT_FOUND", -1}
    end

    local user = cmsgpack.unpack(user_data)

    user.credit = user.credit + amount

    redis.call("SET", userId, cmsgpack.pack(user))
    return {"SUCCESS", user.credit}
    """
)

substract_funds_lua_script = db.register_script(
    """
    local userId = KEYS[1]
    local amount = tonumber(ARGV[1])

    local user_data = redis.call("GET", userId)
    if not user_data then
        return {"USER_NOT_FOUND", -1}
    end

    local user = cmsgpack.unpack(user_data)
    if user.credit < amount then
        return {"INSUFFICIENT_FUNDS", -1}
    end

    user.credit = user.credit - amount
    redis.call("SET", userId, cmsgpack.pack(user))
    return {"SUCCESS", user.credit}                    
    """
)

batch_create_user_lua_script = db.register_script(
    """
    local n = tonumber(ARGV[1])
    local starting_money = tonumber(ARGV[2])
    local kv_pairs = {}

    for i=0,n-1 do
        redis.call("SET", tostring(i), cmsgpack.pack({credit=starting_money}))
    end

    return "SUCCESS"
    """
)

def close_db_connection():
    db.close()


atexit.register(close_db_connection)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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

@app.get('/checkid/{order_id}')
def check_order_id(order_id: str):
    order_key = f"order:{order_id}"
    db_key = db.get(order_key)
    if db_key is None:
        return "MISSING"
    return_data = msgpack.decode(db_key, type=str)
    return return_data


@app.post('/batch_init/{n}/{starting_money}')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    try:
        batch_create_user_lua_script(keys=[], args=[n, starting_money])
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
        raise HTTPException(400, "No such User")

    return {
        "user_id": user_id,
        "credit": user_entry.credit
    }

@app.post('/add_funds/{user_id}/{amount}')
def add_credit(user_id: str, amount: int):
    keys = [user_id]
    args = [amount]
    try:
        result_code, credit = add_funds_lua_script(keys=keys, args=args)
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)

    if result_code == b"USER_NOT_FOUND":
        raise HTTPException(400, "No such user")
    else:
        return Response(f"User: {user_id} credit updated to: {credit}", status_code=200)


@app.post('/pay/{user_id}/{amount}')
def remove_credit(user_id: str, amount: int):
    logging.debug(f"Removing {amount} credit from user: {user_id}")
    try:
        result, credit = substract_funds_lua_script(keys=[user_id], args=[amount])
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
        
    if result == b"USER_NOT_FOUND":
        raise HTTPException(400, "No such user")
    elif result == b"INSUFFICIENT_FUNDS": 
        return Response("Not enough balance", status_code=400)
    elif result == b"SUCCESS":
        return Response(f"User: {user_id} credit updated to: {credit}", status_code=200)
    


if __name__ == '__main__':
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
else:
    logger.info("Starting Payment Service with uvicorn")
