--
-- resharding.lua
--

if perl_crc32 == nil then
    if require == nil then
        error("Add this module into init.lua or preload perl_crc32 before module invoke")
    else
        require('perl_crc32')
    end
end

--
-- Only one configuration is expected perl tarantool
--
local shards_configuration = nil

local function __get_shard(key)
    if shards_configuration == nil then
        error("No configuration was set")
    end

    if key == nil then
        error("key can't be nil")
    end

    if shards_configuration.shards_indexes == nil then
        local indexes = {}
        for shard_id, shard in ipairs(shards_configuration.shards) do
            for i = shard.begin, shard['end'] do
                indexes[i] = shard_id
            end
        end

        shards_configuration.shards_indexes = indexes
    end

    local crc32 = perl_crc32(key)
    if crc32 == nil then
        error("Unexpected return from perl_crc32 for key " .. key .. " (nil)")
    end

    local shard_id = crc32 % shards_configuration.n_shards
    local configured_shard_id = shards_configuration.shards_indexes[shard_id]
    if configured_shard_id == nil then
        -- The key should stay on current shard
        return nil
    end

    local shard = shards_configuration.shards[configured_shard_id]
    if shard.conn == nil then
        -- connect to remote shard
        shard.conn = box.net.box.new(shard.host, shard.port)
    end

    return shard.conn
end

local function __on_method_invoke(method_name)
    return function (...)
        local args = {...}
        local f_name, key, remote_args, return_args = shards_configuration[method_name](unpack(args))

        local shard = __get_shard(key)
        if shard ~= nil and f_name == nil then
            -- TODO
            error("Can't send native select request: not implemented")
        end

        if shard ~= nil then
            if shard:timeout(shards_configuration.timeout):call(f_name, unpack(remote_args)) == nil then
                print("Timeout happens in " .. method_name .. " method, req = " .. box.cjson.encode(args))
                return nil
            end
        end

        return unpack(return_args)
    end
end

-- global variable can be used by anyone
resharding = {
    --
    -- Function will set shards configuration to implement communication between
    -- master shard (current shard, contains not resharded data) and slaves (shards with a copy of data from master).
    --
    -- conf is a table with following format:
    --   { insert = function () ..., select = function () ..., replace = function () ..., delete = function () ..., shards = {}, n_shards = 10, timeout = 10 }
    -- Where
    --   -- insert is a function to be called on insert request
    --   -- select is a function to be called on select request
    --   -- update is a function to be called on update request
    --   -- delete is a function to be called on delete request
    --      All this functions should return 4 values:
    --          'function_name', key_field, { args... }, { return_args ... }
    --      function_name will be called on slave
    --      key_field will be used to calculate correct shard
    --      args will be sent as a request arguments
    --      return_args will be returned to client
    --      This functions should modify master and tell what function should be called on slave
    --   -- shards is a list of tables with following format:
    --     { begin = 0, end = 128, host = 'mail.ru', port = 13013 }
    --   Where:
    --     begin and end are a sharding keys (this rand used to count a shard)
    --     host and port is a path to remote (slave) shard
    --   -- n_shards is a maximum shard id
    --   -- timeout will be used while send the request to the remote shard (in seconds)
    --
    --  All requests to this shard, with key in range [begin .. end] will be duplicated to the remote shard
    --
    set_configuration = function (c)
        local function critical_error(msg)
            error(msg .. ". Restore configuration")
        end

        local new_conf = {}

        local functions = { 'insert', 'select', 'replace', 'delete' }
        for _, fname in functions do
            if c[fname] == nil or type(c[fname]) ~= 'function' then
                critical_error("Function is required in " .. fname .. " arg of configuration")
            end
            new_conf[fname] = c[fname]
        end

        if c.n_shards == nil or type(c.n_shards) ~= "number" or c.n_shards <= 0 then
            critical_error("Invalid n_shards value: a number greater then 0 expected")
        end
        new_conf.n_shards = c.n_shards

        if c.shards == nil or type(c.shards) ~= 'table' then
            critical_error("Array is required in shards arg of configuration")
        end

        local numbers = { 'begin', 'end', 'port' }

        new_conf.shards = {}
        for i, shard in ipairs(c.shards) do
            if type(shard) ~= 'table' then
                critical_error("Invalid shard #" .. i .. " configuration: table expected")
            end

            local sh = {}
            for _, num in ipairs(numbers) do
                if shard[num] == nil or type(shard[num]) ~= "number" then
                    critical_error("Number is expected in " .. num .. " arg of shard #" .. i)
                end
                sh[num] = shard[num]
            end

            if shard.host == nil or type(shard.host) ~= "string" then
                critical_error("String is expected in host arg of shard #" .. i)
            end
            sh.host = shard.host

            table.insert(new_conf.shards, sh)
        end

        shards_configuration = new_conf
    end,

    --
    -- Following function should be used to replace existed hooks, which are used to modify data in tarantool
    --
    on_insert = __on_method_invoke("insert"),
    on_select = __on_method_invoke("select"),
    on_update = __on_method_invoke("replace"),
    on_delete = __on_method_invoke("delete"),
}
