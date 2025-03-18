-- Decrements the stock of multiple items atomically.
local items = cjson.decode(ARGV[1])

for item_id, quantity in pairs(items) do
    local value = redis.call("GET", item_id)
    if (not value) then
        return redis.error_reply("LuaStockTransaction: item '" .. key .. "' not found")
    end

    local item = cjson.decode(value)
    if (type(item.stock) ~= "number") then
        return redis.error_reply("LuaStockTransaction: invalid stock for '" .. key .. "'")
    end

    if (item.stock < quantity) then
        return redis.error_reply("LuaStockTransaction: insufficient stock for '" .. key .. "'")
    end
end

for item_id, quantity in pairs(items) do
    local value = redis.call("GET", item_id)
    local item = cjson.decode(value)
    item.stock = item.stock - quantity
    redis.call("SET", item_id, cjson.encode(item))
end

return "OK"