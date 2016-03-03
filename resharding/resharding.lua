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

if resharding_configuration == nil then
    resharding_configuration = {
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
end

local function set_resharding_configuration_nocheck(first_index, last_index, remote_shard_addr, remote_shard_port, opts)
    resharding_configuration = {
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

    for k, v in pairs(resharding_configuration) do
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

local function call_with_conn(conn, func_name, args)
    local ret = { conn:timeout(resharding_configuration.netbox_timeout):call('resharding.__remote_function_call', func_name, unpack(args)) }
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

    return crc32 % MAX_SHARD_INDEX
end

local function process_request(local_func_name, remote_func_name, key, args, on_send_locally, on_send_remotely)
    local conn = box.net.self
    local func_name = local_func_name

    if resharding_configuration.test_key ~= nil and key ~= resharding_configuration.test_key then
        return call_with_conn(conn, func_name, args)
    end

    local shard_no = -1
    if resharding_configuration.test_key == nil then
        shard_no = calculate_shard_number(key)
    end

    if shard_no == -1 or (shard_no >= resharding_configuration.first_index and shard_no < resharding_configuration.last_index) then
        -- send request on remote shard
        if resharding_configuration.conn == nil then
            resharding_configuration.conn = establish_connection(resharding_configuration.remote_shard_addr, resharding_configuration.remote_shard_port)
        end

        conn = resharding_configuration.conn
        func_name = remote_func_name

        if on_send_remotely then
            on_send_remotely() -- We should log something here
        end
    elseif on_send_locally then
        on_send_locally()
    end

    return call_with_conn(conn, func_name, args)
end

-- @local_func_name
-- @remote_func_name
--  This is a name of the function to be called locally or remotely
-- @key_finder
--  This callback should return unpacked key value from a current function arguments
local function wrap_nocheck(local_func_name, remote_func_name, key_finder)
    print("Wrapper for '" .. local_func_name .. "' enabled, remote name is " .. remote_func_name)
    return function (...)
        local key = key_finder(...)
        if key == nil then
            error("Invalid key for " .. local_func_name .. "(" .. box.cjson.encode({ ... }) .. ")")
        end

        return process_request(local_func_name, remote_func_name, key, { ... })
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
        if shard_no >= resharding_configuration.first_index and shard_no < resharding_configuration.last_index then
            -- Key should be stored on remote shard => delete it
            rows_removed = rows_removed + 1
            print("Trying to remove tuple with key " .. key .. " (hash_func == " .. shard_no .. ")")
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

    resharding_configuration.cleanup_shard_fiber = nil

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
        if first_index == nil or first_index < 0 or first_index > MAX_SHARD_INDEX
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

    -- @local_func_name
    -- @remote_func_name
    --  This is a name of the function to be called locally or remotely
    -- @key_finder
    --  This callback should return unpacked key value from a current function arguments
    wrap = function(local_func_name, remote_func_name, key_finder)
        if local_func_name == nil or type(local_func_name) ~= 'string' or local_func_name == "" or _G[local_func_name] == nil
                or remote_func_name == nil or type(remote_func_name) ~= 'string' or remote_func_name == "" or _G[remote_func_name] == nil
                or key_finder == nil or type(key_finder) ~= 'function' then
            error("Invalid wrapper options!")
        end

        if resharding_configuration.first_index == nil then
            print("ERROR! Set resharding configuration first! Old func is used")
            return function (...)
                return resharding.__remote_function_call(remote_func_name, ...)
            end
        end

        return wrap_nocheck(local_func_name, remote_func_name, key_finder)
    end,

    -- This function will be called by remote tarantool.
    -- We can't just call @func_name because box.net:timeout() returns nil on timeout,
    -- but nil can be effective return value for a function.
    -- XXX: function should be in global scope
    __remote_function_call = function (func_name, ...)
        return { _G[func_name](...) }
    end,

    process_request = process_request,

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
        if resharding_configuration.first_index == nil then
            error("Set resharding configuration first!")
        end

        if resharding_configuration.cleanup_shard_fiber ~= nil and box.fiber.status(resharding_configuration.cleanup_shard_fiber) ~= 'dead' then
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

        resharding_configuration.cleanup_shard_fiber = box.fiber.wrap(function ()
            run_cleanup_nocheck(space, index, key_field_no, _opts)
        end)

        resharding_configuration.cleanup_shard_fiber:name("cleanup_shard_fiber_space_" .. space)
        print("Started fiber with id " .. resharding_configuration.cleanup_shard_fiber:id()
            .. ", name == " .. resharding_configuration.cleanup_shard_fiber:name())
    end,
}
