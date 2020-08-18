package scripts

import "github.com/gomodule/redigo/redis"

var Scripts map[string]*redis.Script

func init() {
	Scripts = make(map[string]*redis.Script)

	/***
	  register lua scripts
	*/
	Scripts["size"] = redis.NewScript(3, `return redis.call('llen', KEYS[1]) + redis.call('zcard', KEYS[2]) + redis.call('zcard', KEYS[3])`)

	// Get the Lua script to migrate expired jobs back onto the queue.
	Scripts["pop"] = redis.NewScript(2, `local job = redis.call('lpop', KEYS[1])
local reserved = false
if(job ~= false) then
reserved = cjson.decode(job)
reserved['attempts'] = reserved['attempts'] + 1
reserved = cjson.encode(reserved)
redis.call('zadd', KEYS[2], ARGV[1], reserved)
end
return {job, reserved}`)

	Scripts["migrateExpiredJobs"] = redis.NewScript(2, `
-- Get all of the jobs with an expired "score"...
local val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1])

-- If we have values in the array, we will remove them from the first queue
-- and add them onto the destination queue in chunks of 100, which moves
-- all of the appropriate jobs onto the destination queue very safely.
if(next(val) ~= nil) then
    redis.call('zremrangebyrank', KEYS[1], 0, #val - 1)

    for i = 1, #val, 100 do
        redis.call('rpush', KEYS[2], unpack(val, i, math.min(i+99, #val)))
    end
end

return val`)
}
