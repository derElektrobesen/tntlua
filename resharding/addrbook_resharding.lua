if resharding == nil then
    dofile('resharding.lua')
end

-- XXX: This variables are used to store old functions pointers
--      and should be called remotely.
--      Remote call can't see local values, so don't make them local !!!
addrbook_add_recipient_old = nil
addrbook_get_old = nil
addrbook_put_old = nil
addrbook_delete_old = nil

local default_log_lvl = 5
local default_timeout = 10 -- in seconds

-- This function is used to get userid from args, passed into addrdb_* functions.
-- In all functions, userid is first argument => use single function to
-- get userid for all functions.
local function addrbook_get_uid(userid, ...)
    return box.unpack('i', userid)
end

local function set_handlers()
    addrbook_add_recipient_old = addrbook_add_recipient
    addrbook_get_old = addrbook_get
    addrbook_put_old = addrbook_put
    addrbook_delete_old = addrbook_delete

    addrbook_add_recipient = resharding.invoke(addrbook_add_recipient_old, addrbook_get_uid, 'addrbook_add_recipient_old')
    addrbook_get = resharding.invoke(addrbook_get_old, addrbook_get_uid, 'addrbook_get_old')
    addrbook_put = resharding.invoke(addrbook_put_old, addrbook_get_uid, 'addrbook_put_old')
    addrbook_delete = resharding.invoke(addrbook_delete_old, addrbook_get_uid, 'addrbook_delete_old')
end

-- Restore old addrbook handlers
function addrbook_restore_shard()

    if addrbook_add_recipient_old == nil or
            addrbook_add_recipient_old == nil or
            addrbook_get_old == nil or
            addrbook_put_old == nil then
        error("Can't restore shard configuration! Handlers are possibly corrupted. Restart tarantool in this case")
    end

    addrbook_add_recipient = addrbook_add_recipient_old
    addrbook_get = addrbook_get_old
    addrbook_put = addrbook_put_old
    addrbook_delete = addrbook_delete_old

    addrbook_add_recipient_old = nil
    addrbook_get_old = nil
    addrbook_put_old = nil
    addrbook_delete_old = nil

    log_info("Addrbook restored!")
end

-- Function get a table with following fields:
--  * shards is a list of tables with following format:
--     { i_begin = 0, i_end = 128, host = 'mail.ru', port = 13013 }
--    Where:
--     i_begin and i_end are a sharding indexes (this range used to count a shard)
--     host and port is a path to remote (slave) shard
--  * log_lvl is current log level (default is 5 or look at default_log_lvl)
--  * timeout sets a timeout for a request on the remote shard (default is 10 seconds or look at default_timeout), can be
--     floating-point
--  * dryrun options helps to disable dryrun mode. This mode is enabled by default. Use 'false' constant to disable it.
--
function addrbook_set_master_configuration(conf)
    if conf.log_lvl == nil then
        conf.log_lvl = default_log_lvl
        print("Force set log_lvl to " .. conf.log_lvl)
    end
    if conf.timeout == nil then
        conf.timeout = default_timeout
        print("Force set timeout to " .. conf.timeout .. " secs")
    end

    resharding.set_master_configuration(conf)
    set_handlers()
end

--
-- Function get a table with following fields:
--  * host is a master's host
--  * port is a master's port
--  * timeout is a timouet for a remote request (default is 10 seconds or look at default_timeut), can be floating-point
--  * log_lvl is current log level. Default is 5 (or look at default_log_lvl)
--  * dryrun options helps to disable dryrun mode. This mode is enabled by default. Use 'false' constant to disable it.
--
--  * i_begin
--  * i_end
--    This options are a sharding indexes. This range is used to understand which tuples shojuld be stored locally and which
--    remotely.
function addrbook_set_slave_configuration(conf)
    if conf.log_lvl == nil then
        conf.log_lvl = default_log_lvl
        print("Force set log_lvl to " .. conf.log_lvl)
    end
    if conf.timeout == nil then
        conf.timeout = default_timeout
        print("Force set timeout to " .. conf.timeout .. " secs")
    end

    resharding.set_slave_configuration(conf)
    set_handlers()
end

--
-- Function returns an index in tarantool for spammerdb
-- Used, for example, in box.delete request:
--  box.delete(0, uid, email)
--                \--------/
--        should be returned here
--
-- Unpacked tarantool tuple is an argument
local function addrbook_get_index_space_0(uid, ...)
    return uid
end

local function addrbook_get_index_space_1(uid, rcp_email, ...)
    return uid, rcp_email
end

--
-- Function checks current shard content
-- XXX: Execute this function only on replicas !!!!!
--
function addrbook_check_shard(rows_per_sleep, sleep_time)
    if not rows_per_sleep then
        rows_per_sleep = 10000
    end
    if not sleep_time then
        sleep_time = default_timeout -- in seconds
    end

    check_shard(0, 0, addrbook_get_uid, addrbook_get_index_space_0, rows_per_sleep, sleep_time,
        function ()
            -- Will be called when check will be complete
            -- Start check of the next space
            check_shard(1, 0, addrbook_get_uid, addrbook_get_index_space_1, rows_per_sleep, sleep_time)
        end)
end

--
-- Function removes unnecessary records from current tarantool
-- message should be "yes, i'm really sure"
--
function addrbook_cleanup_shard(message, rows_per_sleep, sleep_time)
    if not rows_per_sleep then
        rows_per_sleep = 10000
    end
    if not sleep_time then
        sleep_time = default_timeout
    end

    cleanup_shard(message, 0, 0, addrbook_get_uid, addrbook_get_index_space_0, rows_per_sleep, sleep_time,
        function ()
            -- Will be called when cleanup will be complete
            -- Start cleanup of the next space
            cleanup_shard(message, 1, 0, addrbook_get_uid, addrbook_get_index_space_1, rows_per_sleep, sleep_time)
        end)
end
