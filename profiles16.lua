local log
log = require('log')

function _select(space_name, request_id)
    log.info("_select space = " .. space_name .. " id = " .. request_id)
    return box.space[space_name]:get(request_id)
end

function _delete(space_name, request_id)
    log.info("_delete space = " .. space_name .. " id = " .. request_id)
    return box.space[space_name]:delete(request_id)
end


-- ========================================================================= --
-- Profile local functions
-- ========================================================================= --

local function print_profile(header, profile)
    if not profiles._debug then
        return
    end
    print(header)
    print("id = ", profile.id)
    for pref_id, pref_value in pairs(profile.prefs) do
        print("prefs[", pref_id, "] = ", pref_value)
    end
end

local function load_profile_from_tuple(tuple)
    local profile = {}

    profile.id = tuple[1]
    local k = tuple:next() -- skip user id

    -- fill preference list
    profile.prefs = {}
    while k ~= nil do
        local pref_key, pref_value
        -- get preference key
        k, pref_key = tuple:next(k)
        if k == nil then break end
        -- get preference value
        k, pref_value = tuple:next(k)
        if k == nil then break end
        -- put preference
        profile.prefs[pref_key] = pref_value
    end

    return profile
end

local function load_profile(profile_id)
    local profile = nil

    -- try to find tuple
    local tuple = box.space.profiles:get(profile_id)
    if tuple ~= nil then
        profile = load_profile_from_tuple(tuple)
    else
        -- init empty profile
        profile = {}
        profile.id = profile_id
        profile.prefs = {}
    end

    print_profile("load", profile)
    return profile
end

local function store_profile(profile)
    print_profile("store", profile)
    -- init profile tuple
    local tuple = { profile.id }
    -- put preference to tuple
    for pref_id, pref_value in pairs(profile.prefs) do
        -- insert preference id
        table.insert(tuple, pref_id)
        -- insert preference value
        table.insert(tuple, pref_value)
    end
    return box.space.profiles:replace(tuple)
end

local function profile_create_empty_with_defaults(profile_id, pref_list)
    local profile_tuple = { profile_id }
    local k, v

    repeat
        k, v = next(pref_list, k)           -- key
        if k == nil then break end

        k, _ = next(pref_list, k)           -- skip value
        if k == nil then break end

        table.insert(profile_tuple, v)      -- already packed
        table.insert(profile_tuple, '1')
    until k == nil

    return box.space.profiles:replace(profile_tuple)
end

-- ========================================================================= --
-- Profile interface
-- ========================================================================= --

profiles = {
    -- enable/disable debug functions
    _debug = false,
}

function profile_get(profile_id)
    return box.space.profiles:get(profile_id)
end

function profile_multiset(profile_id, ...)
    log.info("profile_multiset id = " ..  profile_id)
    local pref_list = {...}
    local pref_list_len = table.getn(pref_list)

    --
    -- check input params
    --

    -- profile id
    if profile_id == nil then
        log.warn("profile's id undefied")
        error("profile's id undefied")
    end
    -- preference list's length
    if pref_list_len == 0 then
        --- nothing to set, return the old tuple (if it exists)
        return box.space.profiles:get(profile_id)
    end
    -- preference list's parity
    if pref_list_len % 2 ~= 0 then
        log.warn("illegal parameters")
        error("illegal parameters: var arguments list should contain pairs pref_key pref_value")
    end

    --
    -- process
    --

    -- load profile
    local profile = load_profile(profile_id)

    -- initialize iterator by pref argument list, all arguments go by pair
    -- id and value.
    local i, pref_key = next(pref_list)
    local i, pref_value = next(pref_list, i)
    while pref_key ~= nil and pref_value ~= nil do
        -- erase preference if its new value is empty string
        if pref_value == '' then
            pref_value = nil
        end
        -- set new preference value
        profile.prefs[pref_key] = pref_value
        -- go to the next pair
        i, pref_key = next(pref_list, i)
        i, pref_value = next(pref_list, i)
    end

    -- store result
    return store_profile(profile)
end

function profile_set(profile_id, pref_key, pref_value)
    return profile_multiset(profile_id, pref_key, pref_value)
end

function profile_increment_multi(profile_id, ...)
    local pref_list = {...}
    local pref_list_len = table.getn(pref_list)

    -- check input params
    if profile_id == nil then
        error("profile's id undefined")
    end

    if pref_list_len == 0 then
        error("no preferences found to inc")
    end

    if pref_list_len % 2 ~= 0 then
        error("illegal parameters: var arguments list should contain pairs pref_key pref_value")
    end

    -- request a profile tuple from storage
    local profile_tuple = box.space.profiles:get(profile_id)
    if profile_tuple == nil then
        -- profile is not exists yet. Create it with defaults
        return profile_create_empty_with_defaults(profile_id, pref_list)
    end

    -- make a dict from pref_list
    local pref_dict = {}
    local k = nil

    repeat
        local pref_key, pref_value

        k, pref_key = next(pref_list, k)
        if k == nil then break end

        -- no need to check boundary: even number of items
        k, pref_value = next(pref_list, k)

        -- put preference into dict
        -- don't unpack the key: in tarantool key is packed too
        -- if pref_value is not a number, tonumber() will return nill and the pref_key will be ignored
        pref_dict[pref_key] = tonumber(pref_value)
    until k == nil

    -- variables to store replace arguments
    local replace_args = {}

    local add_replacement = function (index, key, val, incremented)
        if not incremented then
            -- need to set unexisted value. Add a key into tuple
            table.insert(replace_args, { '=', index, key })
        end

        table.insert(replace_args, { '=', index + 1, val })

        -- in case of tuple extend, returns new tuple length
        return index + 2
    end

    -- skip user id
    k, _ = profile_tuple:next()

    -- index to make a replacement
    local index = 2

    -- main loop
    while k ~= nil do
        local pref_key, pref_value

        k, pref_key = profile_tuple:next(k)
        if k == nil then
            -- no more items in profile tuple
            break
        end

        k, pref_value = profile_tuple:next(k)

        -- find a key in given preferences
        local inc_val = pref_dict[pref_key]
        if inc_val ~= nil then
            -- found a key to increment
            -- save item to store changes in tarantool
            if pref_value == nil then
                -- set default value
                add_replacement(index, pref_key, '1')
            else
                pref_value = tonumber(pref_value)
                if pref_value ~= nil then
                    -- increment value by inc_val
                    add_replacement(index, pref_key, tostring(pref_value + inc_val), true)
                end
            end

            -- 'remove' updated key from pref_dict
            pref_dict[pref_key] = nil
        end

        index = index + 2
    end

    -- add still unexisted requested keys into profile with default value
    for k, v in pairs(pref_dict) do
        if k ~= nil and v ~= nil then
            -- still not processed item: add with default value
            index = add_replacement(index, k, '1')
        end
    end

    if table.getn(replace_args) == 0 then
        -- shouldn't be
        -- TODO: make me error
        log.warn("nothing to increment")
        return profile_tuple
    end

    -- replace tuple on storage
    return box.space.profiles:update(profile_id, replace_args)
end

function profile_increment(profile_id, pref_key, inc_val)
    return profile_increment_multi(profile_id, pref_key, inc_val)
end

-- ========================================================================== --
-- helper functions which help to delete empty preferences
-- ========================================================================== --

local function profile_cleanup_empty_preferences()
    local cnt = 0
    local n = 0

    for _, tpl in box.space.profiles.index[0]:pairs(nil, {iterator=box.index.ALL}) do
        local profile = load_profile_from_tuple(tpl)
        local has_empty = false

        for k, v in pairs(profile.prefs) do
            if v == '' then
                profile.prefs[k] = nil
                has_empty = true
            end
        end

        if has_empty == true then
            store_profile(profile)
            cnt = cnt + 1
        end

        n = n + 1
        if n == 100 then
            box.fiber.sleep(0.001)
            n = 0
        end
    end

    print(cnt, ' tuples cleaned from empty keys')
    return cnt
end

-- ========================================================================== --
-- cleans up keys with empty string values in every profile
-- needs to be called once for old version of profiles.lua
-- new version no longer produces keys with empty string values
-- ========================================================================== --

function profile_cleanup_empty_preferences_all()
    while profile_cleanup_empty_preferences() ~= 0 do end
end

-- ========================================================================== --
-- helper functions which removes particular key in every profile
-- ========================================================================== --
local function profile_hexify(str)
    return (
        string.gsub(str, "(.)",
            function (c) return string.format("%02X", string.byte(c)) end
        )
    )
end

local function profile_cleanup_key(key)
    local cnt = 0
    local n = 0

    for _, tpl in box.space.profiles.index[0]:pairs(nil, {iterator=box.index.ALL}) do
        local profile = load_profile_from_tuple(tpl)
        if profile.prefs[key] ~= nil then
            print('cleanup_key: ', profile.id, '[', profile_hexify(key), ']', ' = ', profile.prefs[key], ', orig tuple: ', tpl)
            profile.prefs[key] = nil
            store_profile(profile)
            cnt = cnt + 1
        end

        n = n + 1
        if n == 100 then
            box.fiber.sleep(0.001)
            n = 0
        end
    end

    print(cnt, ' tuples cleaned from key [', profile_hexify(key), ']')
    return cnt
end

-- ========================================================================== --
-- cleans up particular key in every profile
-- ========================================================================== --

function profile_cleanup_key_all(key)
    if type(key) ~= "string" then
        error("bad parameters")
    end

    while profile_cleanup_key(key) ~= 0 do end
end

-- ========================================================================== --
-- profile replica admin functions
-- ========================================================================== --

local function tnt_bug_i64(d)
    return tostring(d):sub(1, -4)
end

local function profile_id_to_int(id)
    if #id == 4 then
        return box.unpack("i", id)
    elseif #id == 8 then
        return tnt_bug_i64(box.unpack("l", id))
    else
        error("bad profile id")
    end
end

local function profile_apply_func(func, ...)
    local cnt = 0

    for _, tpl in box.space.profiles.index[0]:pairs(nil, {iterator=box.index.ALL}) do
        local profile = load_profile_from_tuple(tpl)
        func(profile, ...)

        cnt = cnt + 1
        if cnt == 1000 then
            box.fiber.sleep(0)
            cnt = 0
        end
    end
end

function profile_print_str_key(key_id)
    if box.cfg.replication_source == nil then error("replica api only") end
    if type(key_id) == "string" then key_id = tonumber(key_id) end

    profile_apply_func(
        function(p, key_id)
            local str_key_id = box.pack("w", key_id)
            print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: ", string.format("%s", p.prefs[str_key_id]))
        end,
        key_id
    )
end

function profile_print_int_key(key_id)
    if box.cfg.replication_source == nil then error("replica api only") end
    if type(key_id) == "string" then key_id = tonumber(key_id) end

    profile_apply_func(
        function(p, key_id)
            local str_key_id = box.pack("w", key_id)
            local val = "nil"
            if p.prefs[str_key_id] ~= nil then
                if #p.prefs[str_key_id] ~= 4 then
                    print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: BAD")
                    return
                end
                val = box.unpack("i", p.prefs[str_key_id])
            end
            print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: ", val)
        end,
        key_id
    )
end

function profile_print_int64_key(key_id)
    if box.cfg.replication_source == nil then error("replica api only") end
    if type(key_id) == "string" then key_id = tonumber(key_id) end

    profile_apply_func(
        function(p, key_id)
            local str_key_id = box.pack("w", key_id)
            local val = "nil"
            if p.prefs[str_key_id] ~= nil then
                if #p.prefs[str_key_id] ~= 8 then
                    print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: BAD")
                    return
                end
                val = tnt_bug_i64(box.unpack("l", p.prefs[str_key_id]))
            end
            print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: ", val)
        end,
        key_id
    )
end

function profile_print_specific_key(key_id, key_val)
    if box.cfg.replication_source == nil then error("replica api only") end
    if type(key_id) == "string" then key_id = tonumber(key_id) end
    if type(key_val) == "string" then key_val = tonumber(key_val) end

    profile_apply_func(
        function(p)
            local str_key_id = box.pack("w", key_id)
            if p.prefs[str_key_id] == key_val then
                print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: ", key_val)
            end
        end
    )
end

function profile_print_specific_int_key(key_id, key_val)
    if box.cfg.replication_source == nil then error("replica api only") end
    if type(key_id) == "string" then key_id = tonumber(key_id) end
    if type(key_val) == "string" then key_val = tonumber(key_val) end

    profile_apply_func(
        function(p, key_id, key_val)
            local str_key_id = box.pack("w", key_id)
            local str_key_val = box.pack("i", key_val)

            if p.prefs[str_key_id] == str_key_val then
                print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: ", key_val)
            end
        end,
        key_id,
        key_val
    )
end

function profile_print_specific_int64_key(key_id, key_val)
    if box.cfg.replication_source == nil then error("replica api only") end
    if type(key_id) == "string" then key_id = tonumber(key_id) end
    if type(key_val) == "string" then key_val = tonumber(key_val) end

    profile_apply_func(
        function(p, key_id, key_val)
            local str_key_id = box.pack("w", key_id)
            local str_key_val = box.pack("l", key_val)

            if p.prefs[str_key_id] == str_key_val then
                print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: ", key_val)
            end
        end,
        key_id,
        key_val
    )
end

function profile_print_specific_int_key_bit(key_id, bit_n)
    if box.cfg.replication_source == nil then error("replica api only") end
    if type(key_id) == "string" then key_id = tonumber(key_id) end
    if type(bit_n) == "string" then bit_n = tonumber(bit_n) end
    if bit_n < 0 or bit_n > 31 then  error("bad parameters") end
    
    profile_apply_func(
        function(p, key_id, bit_n)
            local str_key_id = box.pack("w", key_id)
            if p.prefs[str_key_id] == nil then
                return
            end
            if #p.prefs[str_key_id] ~= 4 then
                print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: BAD")
                return
            end
            if bit.band(box.unpack("i", p.prefs[str_key_id]), bit.lshift(1, bit_n)) ~= 0 then
                print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: ", box.unpack("i", p.prefs[str_key_id]))
            end
        end,
        key_id,
        bit_n
    )
end

function profile_print_specific_int64_key_bit(key_id, bit_n)
    if box.cfg.replication_source == nil then error("replica api only") end
    if type(key_id) == "string" then key_id = tonumber(key_id) end
    if type(bit_n) == "string" then bit_n = tonumber(bit_n) end
    if bit_n < 0 or bit_n > 63 then  error("bad parameters") end

    profile_apply_func(
        function(p, key_id, bit_n)
            local str_key_id = box.pack("w", key_id)
            if p.prefs[str_key_id] == nil then
                return
            end
            if #p.prefs[str_key_id] ~= 8 then
                print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: BAD")
                return
            end
            local n = 0
            if bit_n >= 32 then
                bit_n = bit_n - 32
                n = box.unpack("i", p.prefs[str_key_id]:sub(-4))
            else
                n = box.unpack("i", p.prefs[str_key_id]:sub(1, 4))
            end
            if bit.band(n, bit.lshift(1, bit_n)) ~= 0 then
                print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: ", box.unpack("l", p.prefs[str_key_id]))
            end
        end,
        key_id,
        bit_n
    )
end

function profile_print_specific_str_int_key_bit(key_id, bit_n)
    if box.cfg.replication_source == nil then error("replica api only") end
    if type(key_id) == "string" then key_id = tonumber(key_id) end
    if type(bit_n) == "string" then bit_n = tonumber(bit_n) end
    if bit_n < 0 or bit_n > 63 then  error("bad parameters") end

    profile_apply_func(
        function(p, key_id, bit_n)
            local str_key_id = box.pack("w", key_id)
            if p.prefs[str_key_id] == nil then
                return
            end
            local n64 = tonumber64(p.prefs[str_key_id])
            if n64 == nil or n64 < 0 then
                print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: BAD")
                return
            end
            local n64str = box.pack("l", n64)
            local n = 0
            if bit_n >= 32 then
                bit_n = bit_n - 32
                n = box.unpack("i", n64str:sub(-4))
            else
                n = box.unpack("i", n64str:sub(1, 4))
            end
            if bit.band(n, bit.lshift(1, bit_n)) ~= 0 then
                print("user_id: ", profile_id_to_int(p.id), " id: ", key_id, " val: ", p.prefs[str_key_id])
            end
        end,
        key_id,
        bit_n
    )
end
