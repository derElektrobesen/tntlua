--
-- spammendb_resharding.lua
--

if resharding == nil then
    dofile('resharding.lua')
end

local old_spammerdb_set = spammerdb_set
local old_spammerdb_get = spammerdb_get
local old_spammerdb_getall = spammerdb_getall
local old_spammerdb_delall = spammerdb_delall

--
--      All this functions should return 4 values:
--          'function_name', key_field, { args... }, { return_args ... }
--      function_name will be called on slave
--      key_field will be used to calculate correct shard
--      args will be sent as a request arguments
--      return_args will be returned to client
--      This functions should modify master and tell what function should be called on slave
--
local function on_spammerdb_set()
    
end

local function on_spammerdb_get(fname, ...)

end

local function on_spammerdb_delall()

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
        select = on_spammerdb_get,
        delete = on_spammerdb_delall,
        replace = function (...) end,
        shards = conf,
        n_shards = 1024,
        timeout = 10,
    }

    resharding.set_configuration(configuration)

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
end

