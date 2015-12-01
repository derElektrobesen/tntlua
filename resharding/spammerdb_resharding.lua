--
-- spammerdb_resharding.lua
--

if resharding == nil then
    dofile('resharding.lua')
end

-- XXX: This variables are used to store old functions pointers
--      and should be called remotely.
--      Remote call can't see local values, so don't make them local !!!
old_spammerdb_set = nil
old_spammerdb_delall = nil

local default_log_lvl = 5
local default_timeout = 10 -- in seconds

-- This function is used to get userid from args, passed into spammerdb_set
-- or spammerdb_delall functions.
-- In both functions, userid is first argument => use single function to
-- get userid from both functions.
local function spammerdb_get_uid(userid, ...)
    return box.unpack('i', userid)
end

local function set_handlers()
    old_spammerdb_set = spammerdb_set
    old_spammerdb_delall = spammerdb_delall

    -- invoke() gets 3 args:
    --      local function to be invoked locally
    --      a function to get userid from passed into function arguments
    --          (needed to calculate shard id)
    --      remote function name to be invoked remotely
    spammerdb_set = resharding.invoke(old_spammerdb_set, spammerdb_get_uid, 'old_spammerdb_set')
    spammerdb_delall = resharding.invoke(old_spammerdb_delall, spammerdb_get_uid, 'old_spammerdb_delall')
end

--
-- conf is a list of tables with following format:
--     { i_begin = 0, i_end = 128, host = 'mail.ru', port = 13013 }
--   Where:
--     i_begin and i_end are a sharding indexes (this rand used to count a shard)
--     host and port is a path to remote (slave) shard
--
function spammerdb_set_master_configuration(conf, log_lvl, timeout, dryrun)
    if log_lvl == nil then
        log_lvl = default_log_lvl
        print("Force set log_lvl to " .. log_lvl)
    end
    if timeout == nil then
        timeout = default_timeout
        print("Force set timeout to " .. timeout)
    end

    local configuration = {
        shards = conf,
        timeout = timeout,
        log_lvl = log_lvl,
        dryrun = dryrun,
    }

    resharding.set_master_configuration(configuration)
    set_handlers()
end

-- Following options are expected in conf:
--      host
--      port
--      timeout
--      log_lvl
--      i_begin
--      i_end
--      disable_dryrun
function spammerdb_set_slave_configuration(conf)
    -- host, port, log_lvl, i_begin, i_end, timeout, dryrun)
    if conf.log_lvl == nil then
        conf.log_lvl = default_log_lvl
        print("Force set log_lvl to " .. conf.log_lvl)
    end

    resharding.set_slave_configuration(conf)
    set_handlers()
end

--
-- Function will restore shard configuration
-- XXX: this function should be used before reloading resharding.lua module or spammerdb_set_*_configuration()
--      function call.
--
function spammerdb_restore_shard()
    if not old_spammerdb_set or not old_spammerdb_delall then
        log_error("Can't restore configuration!!! Call spammerdb_set_{master|slave}_configuration() first")
        return
    end

    spammerdb_set = old_spammerdb_set
    spammerdb_delall = old_spammerdb_delall

    old_spammerdb_set = nil
    old_spammerdb_delall = nil
end

--
-- Function returns an index in tarantool for spammerdb
-- Used, for example, in box.delete request:
--  box.delete(0, uid, email)
--                \--------/
--        should be returned here
--
local function spammerdb_get_index(uid, email, ...)
    return uid, email
end

--
-- Function checks current shard content
-- XXX: Execute this function only on replicas !!!!!
--
function spammerdb_check_shard(rows_per_sleep, sleep_time)
    if not rows_per_sleep then
        rows_per_sleep = 10000
    end
    if not sleep_time then
        sleep_time = 5 -- in seconds
    end

    check_shard(0, 0, spammerdb_get_uid, spammerdb_get_index, rows_per_sleep, sleep_time)
end

--
-- Function removes unnecessary records from current tarantool
--
function spammerdb_cleanup_shard(message, rows_per_sleep, sleep_time)
    if not rows_per_sleep then
        rows_per_sleep = 10000
    end
    if not sleep_time then
        sleep_time = 5 -- in seconds
    end

    cleanup_shard(message, 0, 0, spammerdb_get_uid, spammerdb_get_index, rows_per_sleep, sleep_time)
end
