-- perl_crc32.lua module exports perl_crc32() function
if perl_crc32 == nil then
    if require == nil then
        error("Add this module into init.lua or preload perl_crc32 before module invoke")
    else
        require('perl_crc32')
    end
end

local MAX_SHARD_INDEX = 1024        -- constant from capron
local DEFAULT_NETBOX_TIMEOUT = 1    -- in seconds

local conf = {
    -- This options enables dryrun mode.
    -- If not nil, only records with equal key will be processed.
    test_key = nil,

    -- Indexes, used to understand where we should store current tuple
    -- XXX: for configuration
    --  +-------+--------------------+
    --  | db_id | db_addr            |
    --  +-------+--------------------+
    --  |  1024 | 11.80.232.50:30071 |
    --  +-------+--------------------+
    --  first_index == 1
    --  last_index == 1024
    first_index = nil,
    last_index = nil,

    -- Records with unsuitable keys will be sent on this tarantool
    remote_shard_addr = nil,
    remote_shard_port = nil,

    -- Timeout for box.net.box (in seconds, float numbers are supported)
    netbox_timeout = nil,
}

local function set_resharding_configuration_nocheck(first_index, last_index, remote_shard_addr, remote_shard_port, opts)
    conf = {
        first_index = first_index,
        last_index = last_index,
        remote_shard_addr = remote_shard_addr,
        remote_shard_port = remote_shard_port,

        test_key = opts.test_key,
        netbox_timeout = opts.netbox_timeout
    }

    print("Configuration reloaded")
    print("\tDryrun mode is " .. (opts.test_key ~= nil and "ENABLED" or "DISABLED"))
    print(" ") -- empty line

    for k, v in pairs(conf) do
        print("\t" .. k .. " = " .. v)
    end
end

local function establish_connection(host, port)
    print("Connecting to " .. host .. ":" .. port)
    local conn = box.net.box.new(host, port)

    if conn == nil then
        error("Can't connect to " .. host .. ":" .. port)
    end

    return conn
end

local function call_with_conn(conn, func_name, ...)
    local ret = { conn:timeout(conf.netbox_timeout):call('resharding.__remote_function_call', func_name, ...) }
    if ret[1] == nil then
        error("Request timed out!")
    end

    if conn == box.net.self then
        -- box.net.self:call() returns a table with data
        ret = ret[1]
    end

    return ret
end

local function calculate_shard_number(key)
    local crc32 = perl_crc32(key)
    if crc32 == nil then
        error("Unexpected return from perl_crc32 for key " .. key .. " (nil)")
    end

    return crc32 % MAX_SHARD_INDEX + 1
end

local function process_request(func_name, key, ...)
    local conn = box.net.self
    if conf.test_key ~= nil and key ~= conf.test_key then
        return call_with_conn(conn, func_name, ...)
    end

    local shard_no = -1
    if conf.test_key == nil then
        shard_no = calculate_shard_number(key)
    end

    if shard_no == -1 or shard_no < conf.first_index or shard_no > conf.last_index then
        -- send request on remote shard
        if conf.conn == nil then
            conf.conn = establish_connection(conf.remote_shard_addr, conf.remote_shard_port)
        end
        conn = conf.conn
    end

    return call_with_conn(conn, func_name, ...)
end

-- @func_name
--  This is a name of the function to be called locally or remotely
-- @key_finder
--  This callback should return unpacked key value from a current function arguments
local function wrap_nocheck(func_name, key_finder)
    print("Wrapper for '" .. func_name .. "' enabled")
    return function (...)
        local key = key_finder(...)
        if key == nil then
            error("Invalid key for " .. func_name .. "(" .. box.cjson.encode({ ... }) .. ")")
        end

        return process_request(func_name, key, ...)
    end
end

local function cleanup_shard_impl(space_no, index_no, key_field_no, opts)
    local rows_removed = 0
    local n_rows = box.space[space_no]:len()
    local last_perc = 0
    local row_index = 0

    for row in box.space[space_no].index[index_no]:iterator(box.index.ALL) do
        row_index = row_index + 1

        local key = opts.key_decoder(row[key_field_no])

        local perc = row_index / n_rows * 100 -- print every percent
        if math.floor(perc) ~= last_perc then
            last_perc = math.floor(perc)
            print(last_perc .. "% rows checked...")
        end

        local shard_no = calculate_shard_number(key)
        if shard_no < conf.first_index or shard_no > conf.last_index then
            -- Key should be stored on remote shard => delete it
            rows_removed = rows_removed + 1
            if opts.dryrun == false then
                box.delete(space_no, opts.index_decoder(row))
            end
        end
    end

    return rows_removed
end

-- XXX: Call this function from a fiber !!!
local function run_cleanup_nocheck(space_no, index_no, key_field_no, opts)
    local n_rows = box.space[space_no]:len()
    local rows_removed_total = 0
    local iter_no = 0
    local rows_removed = 0

    -- function modifies iterator and some rows can be skipped
    repeat
        iter_no = iter_no + 1
        print("Trying to cleanup shard (iteration #" .. iter_no .. ")")

        rows_removed = cleanup_shard_impl(space_no, index_no, key_field_no, opts)
        rows_removed_total = rows_removed_total + rows_removed

        if opts.dryrun then
            -- no records really will be removed in this case
            break
        end
    until rows_removed == 0

    conf.cleanup_shard_fiber = nil

    print("Space " .. space_no .. ", index " .. index_no .. " was cleanuped successfully. Results:")
    print(n_rows .. " rows processed (" .. iter_no .. " iterations)")
    print(rows_removed_total .. " rows removed")
    print((n_rows - rows_removed_total) .. " rows skipped")
    print(box.space[space_no]:len() .. " rows left in tarantool")
end

resharding = {
    -- @opts contains a table with following options (can be nil):
    --      netbox_timeout
    --      test_key (this enables dryrun mode)
    set_configuration = function (first_index, last_index, remote_shard_addr, remote_shard_port, opts)
        if first_index == nil or first_index <= 0 or first_index > MAX_SHARD_INDEX
                or last_index == nil or last_index < 0 or last_index > MAX_SHARD_INDEX or first_index >= last_index
                or remote_shard_addr == nil or remote_shard_addr == ""
                or remote_shard_port == nil or type(remote_shard_port) ~= 'number' then
            error("Invalid arguments")
        end

        local _opts = {
            netbox_timeout = DEFAULT_NETBOX_TIMEOUT,
            test_key = nil,
        }

        if opts ~= nil then
            if opts.netbox_timeout ~= nil and opts.netbox_timeout < 0 then
                error("Invalid timeout")
            end
            _opts.netbox_timeout = opts.netbox_timeout

            if opts.test_key ~= nil then
                _opts.test_key = opts.test_key
            end
        end

        set_resharding_configuration_nocheck(first_index, last_index, remote_shard_addr, remote_shard_port, _opts)
    end,

    -- @func_name
    --  This is a name of the function to be called locally or remotely
    -- @key_finder
    --  This callback should return unpacked key value from a current function arguments
    wrap = function(func_name, key_finder)
        if func_name == nil or type(func_name) ~= 'string' or func_name == "" or _G[func_name] == nil
                or key_finder == nil or type(key_finder) ~= 'function' then
            error("Invalid wrapper options!")
        end

        if conf.first_index == nil then
            print("ERROR! Set resharding configuration first! Old func is used")
            return function (...)
                return resharding.__remote_function_call(func_name, ...)
            end
        end

        return wrap_nocheck(func_name, key_finder)
    end,

    -- This function will be called by remote tarantool.
    -- We can't just call @func_name because box.net:timeout() returns nil on timeout,
    -- but nil can be effective return value for a function.
    -- XXX: function should be in global scope
    __remote_function_call = function (func_name, ...)
        return { _G[func_name](...) }
    end,

    --
    -- Function removes unnecessary records from current tarantool
    -- @opts is a table with following options:
    --      rows_per_sleep
    --          number of rows to process before sleep
    --      sleep_time
    --          in seconds
    --      dryrun
    --          use 'dryrun = false' to disable dryrun mode.
    --      key_decoder
    --          This function will be called to decode key, stored in tarantool into lua key.
    --          This function should be used for numeric keys to box.unpack() them.
    --          If function is not set, possibly packed keys will be used
    --          (packed key will be passed as argument)
    --      index_decoder
    --          This function will be called to find a composite key from a tuple (box.tuple will be passed)
    --          If not set, value with index @key_field_no will be used
    --
    cleanup = function (space, index, key_field_no, opts)
        if conf.first_index == nil then
            error("Set resharding configuration first!")
        end

        if conf.cleanup_shard_fiber ~= nil and box.fiber.status(conf.cleanup_shard_fiber) ~= 'dead' then
            error("Can't start cleanup_shard: fiber is already running, status: " .. box.fiber.status(cleanup_shard_fiber)
                .. ", id: " .. box.fiber.id(cleanup_shard_fiber))
        end

        local _opts = {
            dryrun = true,
            rows_per_sleep = 10000,
            sleep_time = 0.1,
            index_decoder = function (tuple) return tuple[key_field_no] end,
        }

        if space == nil or type(space) ~= 'number' or space < 0
                or index == nil or type(index) ~= 'number' or index < 0
                or key_field_no == nil or type(key_field_no) ~= 'number' or key_field_no < 0 then
            error("Invalid arguments!")
        end

        for _, v in ipairs({ 'key_decoder', 'index_decoder' }) do
            if opts[v] ~= nil and type(opts[v]) ~= 'function' then
                error("Invalid " .. v .. ": function is expected")
            elseif opts[v] ~= nil then
                _opts[v] = opts[v]
            end
        end

        if opts.key_decoder == nil then
            print("WARNING: key_decoder is not set! Shard number will be calculated using tarantool data as is")
        end
        if opts.index_decoder == nil then
            print("WARNING: index_decoder is not set! Index fields will be counted using key_field_no")
        end

        if opts.dryrun ~= nil and opts.dryrun == false then
            _opts.dryrun = false
        end

        for _, k in ipairs({ 'sleep_time', 'rows_per_sleep' }) do
            if opts[k] ~= nil and type(opts[k]) == 'number' and opts[k] > 0 then
                _opts[k] = opts[k]
            else
                print("Set default " .. k .. " = " .. _opts[k])
            end
        end

        local old_decoder = _opts.key_decoder
        local rows_processed = 0
        _opts.key_decoder = function (key)
            -- This function will be called on each row processed.
            rows_processed = rows_processed + 1
            if rows_processed % _opts.rows_per_sleep == 0 then
                print(rows_processed .. " rows processed. Sleep " .. _opts.sleep_time .. " seconds...")
                box.fiber.sleep(_opts.sleep_time)
            end

            -- if decoder is not set, return key as is. This is correct for stringified keys.
            return old_decoder and old_decoder(key) or key
        end

        conf.cleanup_shard_fiber = box.fiber.wrap(function ()
            run_cleanup_nocheck(space, index, key_field_no, _opts)
        end)
    end,
}
