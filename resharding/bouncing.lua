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
    --  first_indes == 1
    --  last_index == 1024
    first_index = nil,
    last_index = nil,

    -- Records with unsuitable keys will be sent on this tarantool
    remote_shard_addr = nil,
    remote_shard_port = nil,

    -- Timeout for box.net.box (in seconds, float numbers are supported)
    netbox_timeout = nil,
}

local function set_bouncing_configuration_nocheck(first_index, last_index, remote_shard_addr, remote_shard_port, opts)
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

    for k, v in pairs(conf) do
        print("\t" .. k .. " = " .. v)
    end
end

local function show_tuple(...)
    return box.cjson.encode({ ... })
end

local function establish_connection(host, port)
    print("Connecting to " .. host .. ":" .. port)
    local conn = box.net.box.new(host, port)

    if conn == nil then
        error("Can't connect to " .. host .. ":" .. port)
    end

    return conn
end

local function call_remotely(func_name, args)
    if conf.conn == nil then
        conf.conn = establish_connection(conf.remote_shard_addr, conf.remote_shard_port)
    end

    local ret = conf.conn:timeout(conf.netbox_timeout):call('bouncing.__remote_function_call', func_name, unpack(args))
    if ret == nil then
        error("Can't call " .. func_name .. " (timeout happens)")
    end

    local json = ret:unpack()

    if type(json) ~= 'string' then
        error("Can't call remote function! Invalid return value!")
    end

    ret = box.cjson.decode(json)
    if ret == nil then
        error("Invalid json found in response: " .. json)
    end

    local resp = {}
    for _, v in ipairs(ret) do
        -- XXX: Any table in response will be treated as a tuple
        if type(v) == 'table' then
            v = box.tuple.new(v)
        end
        table.insert(resp, v)
    end

    return unpack(resp)
end

local function call_locally(func_name, ...)
    return _G[func_name](...)
end


local function calculate_shard_number(key)
    local crc32 = perl_crc32(key)
    if crc32 == nil then
        error("Unexpected return from perl_crc32 for key " .. key .. " (nil)")
    end

    return crc32 % MAX_SHARD_INDEX + 1
end

local function process_request(func_name, key, args)
    if conf.test_key ~= nil and key ~= conf.test_key then
        return call_locally(func_name, unpack(args))
    end

    local shard_no = calculate_shard_number(key)

    if shard_no >= conf.first_index and shard_no <= conf.last_index then
        return call_locally(func_name, unpack(args))
    end

    return call_remotely(func_name, args)
end

-- @conf expects following options:
--  func_name
--      This is a name of the function to be called locally and remotely
--  key_finder
--      This callback should return unpacked key value from a current function arguments
local function wrap_nocheck(conf)
    print("Wrapper for '" .. conf.func_name .. "' enabled")
    return function (...)
        local key = conf.key_finder(...)
        if key == nil then
            error("Invalid key for " .. conf.func_name .. "(" .. show_tuple(...) .. ")")
        end

        return process_request(conf.func_name, key, { ... })
    end
end

bouncing = {
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

        set_bouncing_configuration_nocheck(first_index, last_index, remote_shard_addr, remote_shard_port, _opts)
    end,

    -- @conf expects following options:
    --  func_name
    --      This is a name of the function to be called locally and remotely
    --  key_finder
    --      This callback should return unpacked key value from a current function arguments
    wrap = function(conf)
        if conf == nil
                or conf.func_name == nil or conf.func_name == ""
                or conf.key_finder == nil then
            error("Invalid wrapper options!")
        end

        return wrap_nocheck(conf)
    end,

    -- This function will be called by remote tarantool.
    -- We can't just call @func_name because box.net:timeout() returns nil on timeout,
    -- but nil can be effective return value for a function.
    -- XXX: function should be in global scope
    __remote_function_call = function (func_name, ...)
        -- The box.net.box.call is using the binary protocol to pack procedure arguments, and the binary protocol is type agnostic, so it's
        -- recommended to pass all arguments of remote stored procedure calls as strings.
        -- We should pack response into a json to pass it to client.
        local loc_ret = { call_locally(func_name, ...) }
        local ret = {}

        for _, v in ipairs(loc_ret) do
            if type(v) == 'userdata' then
                table.insert(ret, { v:unpack() }) -- we can't send tarantool tuple as is
            elseif type(v) == 'table' then
                error("Tables are not supported in response!")
            else
                table.insert(ret, v)
            end
        end

        return box.cjson.encode(ret)
    end,
}
