--
-- resharding.lua
--

-- perl_crc32.lua module exports perl_crc32() function
if perl_crc32 == nil then
    if require == nil then
        error("Add this module into init.lua or preload perl_crc32 before module invoke")
    else
        require('perl_crc32')
    end
end

--
-- Only one configuration is expected per tarantool
--
local shards_configuration = nil
local MAX_SHARD_INDEX = 1024 -- this value is hardcoded in capron

--
-- LOG functions
--
local function log_trace(msg)
    if shards_configuration == nil or shards_configuration.log_lvl >= 5 then
        print("[T] " .. msg)
    end
end

local function log_info(msg)
    if shards_configuration == nil or shards_configuration.log_lvl >= 3 then
        print("[I] " .. msg)
    end
end

local function log_err(msg)
    if shards_configuration == nil or shards_configuration.log_lvl >= 2 then
        print("[E] " .. msg)
    end
end

local function establish_connection(host, port)
    log_info("Connecting to " .. host .. ":" .. port .. "...")
    return box.net.box.new(host, port)
end

local function calculate_shard_number(key)
    local crc32 = perl_crc32(key)
    if crc32 == nil then
        error("Unexpected return from perl_crc32 for key " .. key .. " (nil)")
    end

    return crc32 % MAX_SHARD_INDEX
end

function __resharding_remote_function_call(function_name, ...)
    log_trace(function_name .. " was invoked remotely")
    _G[function_name](...)

    -- IMPORTANT !!! function with name 'function_name' can't return nil as result !!!
    -- In tarantool 1.5 there is no exceptions in box.net, so any nil return value will be treated as timeout !!!!
    -- See < https://github.com/tarantool/tarantool/blob/stable/src/box/lua/box_net.lua#L268 > for more info.
    --
    -- result of executition of this function will not be used, so we can return anything (but not nil) without problems.
    return 1
end

-- function returns a "shard" that matches the ket specified.
-- if the key shouldn't be sent to another shard, function returns nil.
local function get_shard(key)
    if shards_configuration == nil then
        error("No configuration was set")
    end

    if key == nil then
        error("key can't be nil")
    end

    -- initialize shards indexes if not initialized yet
    -- indexes are needed for consistent sharding (without libguava):
    -- any shard have indexes range, for example 0..100. Minimum index is 0,
    -- maximum is 1023 (hardcoded in capron). CRC32 sum will be devided on maximum shard index
    -- and from this index we will choose specific shard.
    if shards_configuration.shards_indexes == nil then
        local indexes = {}
        for shard_id, shard in ipairs(shards_configuration.shards) do
            for i = shard.i_begin, shard.i_end do
                indexes[i] = shard_id
            end
        end

        shards_configuration.shards_indexes = indexes
    end

    local shard_id = calculate_shard_number(key)
    local configured_shard_id = shards_configuration.shards_indexes[shard_id]
    if configured_shard_id == nil then
        -- The key should stay on current shard
        return nil
    end

    local shard = shards_configuration.shards[configured_shard_id]
    if shard.conn == nil then
        -- connect to remote shard
        shard.conn = establish_connection(shard.host, shard.port)
    end

    return shard
end

-- global variable can be used by anyone
resharding = {
    --
    -- Function will set shards configuration to implement communication between
    -- master shard (current shard, contains not resharded data) and slaves (shards with a copy of data from master).
    --
    -- This configuration will be used only by master to calculate key hash sum and to send a record to the specific shard.
    --
    -- conf is a table with following format:
    --   { shards = {}, timeout = 10, log_lvl = 5, dryrun = true, }
    -- Where
    --   -- dryrun
    --          this flag needed to enable DryRun mode.
    --          In this mode all data will be stored on local shard, independently from result of storing data on remote shard.
    --          Needed to understand replication overhead.
    --   -- shards
    --          a list of tables with following format:
    --              { i_begin = 0, i_end = 128, host = 'mail.ru', port = 13013 }
    --          Where:
    --              -- i_begin and i_end
    --                  these indexes are a sharding indexes (this range used to count a shard number)
    --              -- host and port
    --                  these is a path to remote (slave) shard
    --   -- timeout
    --          will be used while send the request to the remote shard (in seconds)
    --   -- log_lvl
    --          just a log level
    --
    --  All requests to this shard, with key in range [i_begin .. i_end] will be duplicated to the remote shard
    --
    --  XXX: dryrun option is ENABLED BY DEFAULT
    --       Set them manually to disable.
    --
    set_master_configuration = function (c)
        local function critical_error(msg)
            error(msg .. ". Restore configuration")
        end

        log_info("Loading master configuration")

        -- validate numerical values
        local new_conf = { stype = "master", }
        local numbers = { 'timeout', 'log_lvl' }
        for _, num in ipairs(numbers) do
            if c[num] == nil or type(c[num]) ~= "number" or c[num] <= 0 then
                critical_error("Invalid " .. num .. " value: a number greater then 0 is expected")
            end
            new_conf[num] = c[num]
        end

        -- parse boolean variables
        -- XXX: this options are ENABLED by default!!!
        local bool_vars = { 'dryrun' }
        for _, var in ipairs(bool_vars) do
            if c[var] ~= nil and not c[var] then
                new_conf[var] = false
            else
                new_conf[var] = true
            end
        end

        -- validate "shards" option
        if c.shards == nil or type(c.shards) ~= 'table' then
            critical_error("Array is required in shards arg of configuration")
        end

        new_conf.shards = {}
        for index, shard in ipairs(c.shards) do
            if type(shard) ~= 'table' then
                critical_error("Invalid shard #" .. index .. " configuration: table expected")
            end

            -- validate numeric shard options: first index, last index, shard port.
            local numbers = { 'i_begin', 'i_end', 'port' }
            local sh = {}

            for _, num in ipairs(numbers) do
                if shard[num] == nil or type(shard[num]) ~= "number" then
                    critical_error("Number is expected in " .. num .. " arg of shard #" .. index)
                end
                sh[num] = shard[num]
            end

            -- validate shard host
            if shard.host == nil or type(shard.host) ~= "string" then
                critical_error("String is expected in host arg of shard #" .. index)
            end
            sh.host = shard.host

            table.insert(new_conf.shards, sh)
        end

        log_info("Set timeout to " .. new_conf.timeout .. "s")
        log_info("DryRun mode is " .. (new_conf.dryrun and "ENABLED" or "DISABLED"))
        log_info("Master configuration loaded")

        shards_configuration = new_conf
    end,

    --
    -- Following function will be used by slave shard to send all requests to master
    -- Master should be configured only by host/port, log level and timeout (see comment above)
    --
    -- Following options are expected in conf:
    --      host
    --      port
    --      timeout
    --      log_lvl
    --      i_begin
    --      i_end
    --      dryrun
    --
    -- i_begin && i_end args are used just to run shard checking
    --
    set_slave_configuration = function (conf)
        log_info("Loading slave configuration")

        local numbers = {
            port = conf.port,
            timeout = conf.timeout,
            log_lvl = conf.log_lvl,
            i_begin = conf.i_begin,
            i_end = conf.i_end,
        }

        for k, v in pairs(numbers) do
            if v == nil or type(v) ~= "number" or v <= 0 then
                error("Invalid " .. k .. " value: " .. tostring(v) .. " (number greater then 0 is expected)")
            end
        end

        if conf.host == nil or type(conf.host) ~= "string" then
            error("Invalid host given: " .. conf.host .. ", string is expected")
        end

        local is_dryrun = true
        if conf.dryrun ~= nil and conf.dryrun == false then
            is_dryrun = false
        end

        shards_configuration = {
            stype = "slave",
            host = conf.host,
            port = conf.port,
            timeout = conf.timeout,
            log_lvl = conf.log_lvl,
            i_begin = conf.i_begin,
            i_end = conf.i_end,
            dryrun = is_dryrun,
        }

        log_info("Set timeout to " .. conf.timeout .. "s")
        log_info("DryRun mode is " .. (shards_configuration.dryrun and "ENABLED" or "DISABLED"))
        log_info("Slave configuration loaded")
    end,

    --
    -- Following function should be used to replace existed hooks, which are used to modify data in tarantool
    --
    --      cb
    --          this function should modify local storage. Return value of this function will be returned to client.
    --          This module implements only synchronous replication!
    --          In synchronous replication this function will be called only when record already stored remotely (or when dryrun
    --          mode enabled).
    --          XXX: if timeout happened while storing request on the remote shard, local shard shouldn't be modified.
    --               In this case remote shard can apply request after timeout and we will have dissinchronization.
    --
    --      uid_getter
    --          this function will be invoked to get user id from a list of arguments.
    --          Function should return (already unpacked) user id (string or a number).
    --
    --      remote_name
    --          this is a function name (string) to be called remotely.
    --          This function should apply request remotely.
    --
    --
    invoke = function (cb, uid_getter, remote_name)
        if cb == nil or uid_getter == nil or remote_name == nil then
            error("Invalid arguments passed in invoke() method: cb == " .. tostring(cb) ..
                ", uid_getter == " .. tostring(uid_getter) .. ", remote_name == " .. tostring(remote_name))
        end

        local function store_locally(...)
            local start_time = box.time()
            local ret = { cb(...) }
            log_info("Local sub execution time: " .. (box.time() - start_time))
            return unpack(ret)
        end

        -- return a closure that will be invoked by client
        return function (...)
            local total_time = box.time()
            local args = {...}
            local key = uid_getter(unpack(args))

            log_trace("Args came: " .. box.cjson.encode(args))

            if shards_configuration == nil then
                -- shards configuration is not set.
                -- Shouldn't be. Someone have miss something.
                -- Store nothing locally and return an error.
                error("Invalid shard configuration")
            end

            local conn, host, port = nil, nil, nil

            if shards_configuration.stype == "master" then
                -- "master" should pass data to remote shard
                -- and then store data locally.
                -- Remote shard should be choosed using a key
                local shard = get_shard(key)
                if shard ~= nil then
                    -- data should be passed on remote shard
                    -- connection is already established
                    conn, host, port = shard.conn, shard.host, shard.port
                end
            elseif shards_configuration.stype == "slave" then
                -- "slave" should pass all requests to "master".
                -- Connection can be still not established
                host, port = shards_configuration.host, shards_configuration.port
                if shards_configuration.conn == nil then
                    shards_configuration.conn = establish_connection(host, port)
                end
                conn = shards_configuration.conn
            else
                error("Unexpected shard type found: " .. shards_configuration.stype .. ". master or slave is expected")
            end

            local to_return = { nil }
            if conn ~= nil then
                log_info("Sending request to shard " .. host .. ":" .. port .. " (key = " .. key .. ") using function " .. remote_name)

                local start_time = box.time()
                local ret = conn:timeout(shards_configuration.timeout):call('__resharding_remote_function_call', remote_name, unpack(args))
                log_info("Remote sub execution time: " .. (box.time() - start_time) .. "[" .. host .. ":" .. port .. "]")

                if ret == nil then
                    -- Request timeouted
                    log_err("Timeout happens in " .. remote_name .. " method, req = " .. box.cjson.encode(args))
                end

                -- when remote transaction complete or when dryrun mode enabled we should apply transaction locally
                if ret ~= nil or shards_configuration.dryrun then
                    to_return = { store_locally(unpack(args)) }
                    if ret == nil then
                        log_info("Timeout happens, but transaction applied locally because DryRun mode is enabled")
                    else
                        log_info(remote_name .. " was successfully invoked on " .. host .. ":" .. port)
                    end
                end
            else
                -- conn is nil
                -- just apply transaction locally
                to_return = { store_locally(unpack(args)) }
                log_info("Key " .. key .. " will not be updated on remote shard")
                host = 'localhost'
                port = '0'
            end

            log_info("Total request execution time: " .. (box.time() - total_time).. "[" .. host .. ":" .. port .. "]")
            return unpack(to_return)
        end
    end,
}

--
-- function compares expected and given data.
-- Expected data can be found on local shard and given data can be
-- found on remote one.
--
-- expected and given have a type of box.tuple
--
-- Function returns true if rows are equal and false otherwise
--
local function check_row(expected, given)
    if given == nil then
        log_err("remote data not found: " .. box.cjson.encode({ expected:unpack() }))
        return false
    end

    if #expected ~= #given then
        log_err("Rows sizes mismatches: " .. box.cjson.encode({ expected:unpack() }) .. " and " .. box.cjson.encode({ given:unpack() }))
        return false
    else
        for k, v in ipairs({ expected:unpack() }) do
            if v ~= given[k - 1] then
                log_err("Rows mismatches: " .. box.cjson.encode({ expected:unpack() }) .. " and " .. box.cjson.encode({ given:unpack() }))
                return false
            end
        end
    end

    return true
end

--
-- See check_shard() for more info
--
-- XXX: Wrap this function into fiber !!!
--
local function check_slave_shard(space_no, index_no, key_cb, index_cb, master_host, master_port, i_begin, i_end)
    local conn = box.net.box.new(master_host, master_port)

    local rows_count = 0
    local rows_skipped = 0
    local rows_ok = 0

    local n_rows = box.space[space_no]:len()
    local row_no = 0
    local last_perc = 0

    for row in box.space[space_no].index[index_no]:iterator(box.index.ALL) do
        local key = key_cb(row:unpack())
        row_no = row_no + 1
        local perc = row_no / n_rows * 100 -- print every percent
        if math.floor(perc) ~= last_perc then
            last_perc = math.floor(perc)
            log_info(last_perc .. "% rows checked...")
        end

        local shard_no = calculate_shard_number(key)
        if shard_no >= i_begin and shard_no < i_end then
            -- XXX: No timeout! function should be called by hands
            local remote_data = conn:select(space_no, index_no, index_cb(row:unpack()))
            rows_count = rows_count + 1

            if check_row(row, remote_data) then
                rows_ok = rows_ok + 1
            end
        else
            rows_skipped = rows_skipped + 1
        end
    end

    return {
        total = (rows_count + rows_skipped),
        skipped = rows_skipped,
        checked = rows_count,
        ok = rows_ok,
        fail = (rows_count - rows_ok),
    }
end

--
-- See check_shard() for more info
--
-- XXX: Wrap this function into fiber !!!
--
local function check_master_shard(space_no, index_no, key_cb, index_cb)
    local rows_count = 0
    local rows_ok = 0
    local rows_skipped = 0

    local n_rows = box.space[space_no]:len()
    local last_perc = 0

    for row in box.space[space_no].index[index_no]:iterator(box.index.ALL) do
        rows_count = rows_count + 1
        local key = key_cb(row:unpack())

        local perc = rows_count / n_rows * 100 -- print every percent
        if math.floor(perc) ~= last_perc then
            last_perc = math.floor(perc)
            log_info(last_perc .. "% rows checked...")
        end

        local shard = get_shard(key)
        if shard ~= nil then
            -- XXX: No timeout! function should be called by hands
            local remote_data = shard.conn:select(space_no, index_no, index_cb(row:unpack()))

            if check_row(row, remote_data) then
                rows_ok = rows_ok + 1
            end
        else
            rows_skipped = rows_skipped + 1
        end
    end

    return {
        total = rows_count,
        skipped = rows_skipped,
        checked = (rows_count - rows_skipped),
        ok = rows_ok,
        fail = (rows_count - rows_skipped - rows_ok),
    }
end

--
-- Function will iterate over all records, stored on shard, will count
-- shard id for a specific key and will compare record stored locally
-- with record on the remote shard.
--
--      space_no
--          Space number to check
--      index_no
--          Index number to check (tarantool index)
--      key_cb
--          this callback should take a tuple (unpacked) and return
--          a key fields (in lua representation)
--      index_cb
--          this callback should take a tuple (unpacked) and return
--          an index tuple to be used in box.select() request
--              box.select(0, 0, uid, email)
--                               \--------/
--                         should be returned here
--      on_finish
--          this callback will be called when check is done (nothing will be called if nil)
--
local check_shard_fiber = nil
function check_shard(space_no, index_no, key_cb, index_cb, rows_per_sleep, sleep_time, on_finish)
    -- key_cb will be called for each row => sleep in it
    local extra_args = {}
    local func = check_master_shard

    if shards_configuration.stype == "slave" then
        func = check_slave_shard
        extra_args = { shards_configuration.host, shards_configuration.port, shards_configuration.i_begin, shards_configuration.i_end }
    end

    if on_finish ~= nil then
        local real_func = func
        func = function (...)
            real_func(...)
            log_info("Shard checking complete!")
            on_finish()
        end
    end

    if check_shard_fiber then
        if box.fiber.status(check_shard_fiber) ~= 'dead' then
            error("Can't start check_shard: fiber is already running, status: " .. box.fiber.status(check_shard_fiber) .. ", id: " .. box.fiber.id(check_shard_fiber))
        end
    end

    local row_no = 0
    check_shard_fiber = box.fiber.wrap(
        function (...)
            -- main function to be called
            local res = func(...)
            log_info("Shard cheking complete")
            if on_finish ~= nil then
                -- Needed, for example, to start another shace check
                on_finish()
            end

            log_info("Space " .. space_no .. ", index " .. index_no .. " check results:")
            log_info("Total rows: " .. res.total)
            log_info("Rows skipped: " .. res.skipped)
            log_info("Rows checked: " .. res.checked)
            log_info("Rows ok: " .. res.ok)
            log_info("Invalid rows: " .. res.fail)
        end,
        space_no,
        index_no,
        function (...)
            -- key_cb called
            row_no = row_no + 1
            if row_no % rows_per_sleep == 0 then
                log_info(row_no .. " rows processed. Sleep " .. sleep_time .. " seconds...")
                box.fiber.sleep(sleep_time)
            end

            return key_cb(...)
        end,
        index_cb,
        unpack(extra_args))

    log_info("Shard checking started. Use stop_check_shard() to stop this check")
end

function stop_check_shard()
    local info = ""
    if check_shard_fiber then
        local fiber_id = box.fiber.id(check_shard_fiber)
        box.fiber.cancel(check_shard_fiber)
        info = ", status: " .. box.fiber.status(check_shard_fiber) .. ", id: " .. fiber_id
    end
    log_info("Check shard is cancelled" .. info)
end

-- XXX: Call this function from a fiber!!!
local function cleanup_shard_impl(space_no, index_no, key_cb, index_cb, rows_per_sleep, sleep_time)
    local n_rows = box.space[space_no]:len()
    local row_index = 0
    local last_perc = 0
    local rows_removed = 0

    for row in box.space[space_no].index[index_no]:iterator(box.index.ALL) do
        row_index = row_index + 1
        local key = key_cb(row:unpack())

        local perc = row_index / n_rows * 100 -- print every percent
        if math.floor(perc) ~= last_perc then
            last_perc = math.floor(perc)
            log_info(last_perc .. "% rows checked...")
        end

        local shard = get_shard(key)
        if shard ~= nil then
            rows_removed = rows_removed + 1
            log_trace("Trying to delete row " .. box.cjson.encode({ row:unpack() }))

            box.delete(space_no, index_cb(row:unpack()))
        end
    end

    return rows_removed
end

--
-- Function iterates over all records in tarantool and REMOVES records with key from other shards.
-- Call this function with "yes, i'm really sure" first argument ifg you are really sure to do this.
--
--      space_no
--          Space number to check
--      index_no
--          Index number to check (tarantool index)
--      key_cb
--          this callback should take a tuple (unpacked) and return
--          a key fields (in lua representation)
--      index_cb
--          this callback should take a tuple (unpacked) and return
--          an index tuple to be used in box.delete() request
--              box.delete(0, uid, email)
--                            \--------/
--                      should be returned here
--      on_finish
--          this function will be called when all rows will be removed.
--          Should be used to restart cleanup, for example, for other space
--
local cleanup_shard_fiber = nil
function cleanup_shard(message, space_no, index_no, key_cb, index_cb, rows_per_sleep, sleep_time, on_finish)
    if shards_configuration == nil then
        error("Call set_{master|slave}_configuration first!")
    end

    local exp_msg = "yes, i'm really sure"
    if not message or message ~= exp_msg then
        error('You are not sure you should cleanup shard! Type "' .. exp_msg .. '" (as argument) if it is not')
    end

    -- key_cb will be called for each row => sleep in it
    if cleanup_shard_fiber then
        if box.fiber.status(cleanup_shard_fiber) ~= 'dead' then
            error("Can't start cleanup_shard: fiber is already running, status: " .. box.fiber.status(cleanup_shard_fiber) .. ", id: " .. box.fiber.id(cleanup_shard_fiber))
        end
    end

    local row_no = 0
    local n_rows = box.space[space_no]:len()
    local rows_removed_total = 0
    local iter_no = 0
    cleanup_shard_fiber = box.fiber.wrap(function ()
        local rows_removed = 0
        -- we should repeat cleanup many times because we remove tuples in the iterator loop.
        repeat
            iter_no = iter_no + 1
            log_info("Trying to cleanup shard (iteration " .. iter_no .. ")")
            rows_removed = cleanup_shard_impl(space_no,
                index_no,
                function (...)
                    -- key_cb should be called
                    row_no = row_no + 1
                    if row_no % rows_per_sleep == 0 then
                        log_info(row_no .. " rows processed. Sleep " .. sleep_time .. " seconds...")
                        box.fiber.sleep(sleep_time)
                    end

                    return key_cb(...)
                end,
                index_cb)
                rows_removed_total = rows_removed_total + rows_removed
        until rows_removed == 0

        if on_finish then
            -- For expample, restart cleanup for the next space
            on_finish()
        end

        log_info("Space " .. space_no .. ", index " .. index_no .. " was cleanuped successfully. Results:")
        log_info(n_rows .. " rows processed (" .. iter_no .. " iterations)")
        log_info(rows_removed_total .. " rows removed")
        log_info((n_rows - rows_removed_total) .. " rows skipped")
        log_info(box.space[space_no]:len() .. " rows left in tarantool")
    end)
end
