-- Atomically sets v1 and v2 profile cache entries
local v1Key   = KEYS[1] -- v1 profile hash key
local v2Key   = KEYS[2] -- v2 profile hash key
local v1Field = ARGV[1] -- v1 profile version
local v1Value = ARGV[2] -- v1 profile data
local v2Field = ARGV[3] -- v2 profile version
local v2Value = ARGV[4] -- v2 profile data

if #v1Field > 0 then
    redis.call("HSET", v1Key, v1Field, v1Value)
end

if #v2Field > 0 then
    redis.call("HSET", v2Key, v2Field, v2Value)
end
