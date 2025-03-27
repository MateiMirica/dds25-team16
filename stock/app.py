import logging
import os
import atexit
import uuid
import json

import redis
import uvicorn

from msgspec import msgpack, Struct
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from faststream.kafka.fastapi import KafkaRouter
from stock_worker import StockValue, StockWorker

DB_ERROR_STR = "DB error"

app = FastAPI(title="stock-service")
router = KafkaRouter("kafka:9092", logger=None)
app.include_router(router)


db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

add_stock_lua_script = db.register_script(
    """
    local stockId = KEYS[1]
    local amount = tonumber(ARGV[1])

    local stock_data = redis.call("GET", stockId)
    if not stock_data then
        return {"ITEM_NOT_FOUND", -1}
    end

    local stock = cmsgpack.unpack(stock_data)

    stock.stock = stock.stock + amount

    redis.call("SET", stockId, cmsgpack.pack(stock))
    return {"SUCCESS", stock.stock}
    """
)

substract_stock_lua_script = db.register_script(
    """
    local stockId = KEYS[1]
    local amount = tonumber(ARGV[1])

    local stock_data = redis.call("GET", stockId)
    if not stock_data then
        return {"ITEM_NOT_FOUND", -1}
    end

    local stock = cmsgpack.unpack(stock_data)
    if stock.stock < amount then
        return {"INSUFFICIENT_STOCK", -1}
    end

    stock.stock = stock.stock - amount
    redis.call("SET", stockId, cmsgpack.pack(stock))
    return {"SUCCESS", stock.stock}                    
    """
)

batch_create_stock_lua_script = db.register_script(
    """
    local n = tonumber(ARGV[1])
    local starting_stock = tonumber(ARGV[2])
    local item_price = tonumber(ARGV[3])

    local kv_pairs = {}
    for i=0,n-1 do
        kv_pairs[tostring(i)] = cmsgpack.pack({stock=starting_stock, price=item_price})
    end
    
    redis.call("MSET", unpack(kv_pairs))
    return "SUCCESS"
    """
)

def close_db_connection():
    db.close()


atexit.register(close_db_connection)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
sstockWorker = StockWorker(logger, db, router)

def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        raise HTTPException(400, f"Item: {item_id} not found!")
    return entry


@app.post('/item/create/{price}')
def create_item(price: int):
    key = str(uuid.uuid4())
    logging.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    return {'item_id': key}

@app.post('/batch_init/{n}/{starting_stock}/{item_price}')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    try:
        batch_create_stock_lua_script(keys=[], args=[n, starting_stock, item_price])
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    return {"msg": "Batch init for stock successful"}


@app.get('/find/{item_id}')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return {
        "stock": item_entry.stock,
        "price": item_entry.price
    }


@app.post('/add/{item_id}/{amount}')
def add_stock(item_id: str, amount: int):
    # update stock, serialize and update database
    keys = [item_id]
    args = [amount]
    try:
        result_code, stock = add_stock_lua_script(keys=keys, args=args)
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    if result_code == b"ITEM_NOT_FOUND":
        raise HTTPException(400, DB_ERROR_STR)
    else:
        return Response(f"Item: {item_id} stock updated to: {stock}", status_code=200)


@app.post('/subtract/{item_id}/{amount}')
def remove_stock(item_id: str, amount: int):
    try:
        result, stock = substract_stock_lua_script(keys=[item_id], args=[amount])
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    
    if result == b"ITEM_NOT_FOUND":
        raise HTTPException(400, f"Item: {item_id} not found!")
    elif result == b"INSUFFICIENT_STOCK":
        raise HTTPException(400, f"Item: {item_id} has insufficient stock!")
    elif result == b"SUCCESS":
        return Response(f"Item: {item_id} stock updated to: {stock}", status_code=200)
if __name__ == '__main__':
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
else:
    logger.info("Starting Stock Service with uvicorn")
