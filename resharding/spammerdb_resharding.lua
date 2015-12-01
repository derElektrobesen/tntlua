--
-- spammendb_resharding.lua
--

if resharding == nil then
    dofile('resharding.lua')
end

local old_spammerdb_set = nil
local old_spammerdb_get = nil
local old_spammerdb_getall = nil
local old_spammerdb_delall = nil

--
-- All this functions should return 4 values:
--     'function_name', key_field, { args... }, { return_args ... }
-- function_name will be called on slave
-- key_field will be used to calculate correct shard
-- args will be sent as a request arguments
-- return_args will be returned to client
-- This functions should modify master and tell what function should be called on slave
--
local function on_spammerdb_set(userid, stype, email, value)
    local ret = { old_spammerdb_set(userid, stype, email, value) }
    return 'spammerdb_set', userid, { userid, stype, email, value }, ret
end

local function on_spammerdb_get(userid, stype, ...)
    local args = { userid, stype, ... }
    local ret = { old_spammerdb_get(unpack(args)) }
    return 'spammerdb_get', userid, args, ret
end

local function on_spammerdb_getall(userid)
    local ret = { old_spammerdb_getall(userid) }
    return 'spammerdb_getall', userid, { userid }, ret
end

local function on_spammerdb_delall(userid)
    -- just call native function
    local ret = { old_spammerdb_delall(userid) }
    return 'spammerdb_delall', userid, { userid }, ret
end

local function on_spammerdb_get_multi(fname, ...)
    if fname == "spammendb_get" then
        return on_spammerdb_get(...)
    elseif fname == "spammendb_getall" then
        return on_spammerdb_getall(...)
    else
        error("Invalid function name came into on_spammerdb_get_multi: " .. fname)
    end
end

--
-- conf is a list of tables with following format:
--     { begin = 0, end = 128, host = 'mail.ru', port = 13013 }
--   Where:
--     begin and end are a sharding keys (this rand used to count a shard)
--     host and port is a path to remote (slave) shard
--
function spammerdb_resharding_set_configuration(conf)
    local configuration = {
        insert = on_spammerdb_set,
        select = on_spammerdb_get_multi,
        delete = on_spammerdb_delall,
        replace = function (...) end,
        shards = conf,
        n_shards = 1024,
        timeout = 10,
    }

    resharding.set_configuration(configuration)

    old_spammerdb_set = spammerdb_set
    old_spammerdb_get = spammerdb_get
    old_spammerdb_getall = spammerdb_getall
    old_spammerdb_delall = spammerdb_delall

    spammerdb_set = resharding.on_insert
    spammendb_get = function (...) return resharding.on_select("spammerdb_get", ...) end
    spammendb_getall = function (...) return resharding.on_select("spammerdb_getall", ...) end
    spammerdb_delall = resharding.on_delete
end

function restore_shard()
    spammerdb_set = old_spammerdb_set
    spammendb_get = old_spammerdb_get
    spammendb_getall = old_spammerdb_getall
    spammerdb_delall = old_spammerdb_delall

    old_spammerdb_set = nil
    old_spammendb_get = nil
    old_spammendb_getall = nil
    old_spammerdb_delall = nil
end

