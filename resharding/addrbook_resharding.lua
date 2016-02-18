dofile('resharding.lua')

local functions_names = {
    'addrbook_add_recipient',
    'addrbook_get_recipients',
    'addrbook_get',
    'addrbook_put',
    'addrbook_delete',
}

-- This function is used to get userid from args, passed into addrbook_* functions.
-- In all functions, userid is first argument => use single function to
-- get userid for all functions.
local function addrbook_get_uid(userid, ...)
    return box.unpack('i', userid)
end

function addrbook_enable_resharding()
    for _, v in ipairs(functions_names) do
        if _G[v .. '_old'] ~= nil then
            error('Can\'t enable configuration! Handlers are already set. Disable resharding first.')
        end
    end

    for _, v in ipairs(functions_names) do
        _G[v .. '_old'] = _G[v]
        _G[v] = resharding.wrap(v .. '_old', v, addrbook_get_uid)
    end

    print("Addrbook resharding enabled!")
end

-- Restore old addrbook handlers
function addrbook_disable_resharding()
    for _, v in ipairs(functions_names) do
        if _G[v .. '_old'] == nil then
            error('Can\'t restore shard configuration! Resharding is already disabled or handlers are corrupted. Restart tarantool in this case')
        end
    end

    for _, v in ipairs(functions_names) do
        _G[v] = _G[v .. '_old']
        _G[v .. '_old'] = nil
    end

    print("Addrbook resharding disabled!")
end

-- manually pass @is_dryrun == false to disable dryrun mode
-- conf is { rows_per_sleep = 10000 (default), sleep_time = 0.1 }
function addrbook_cleanup_shard(space_no, conf, is_dryrun)
    local index_decoder = function (tuple)
        return box.unpack('i', tuple[0])
    end

    if space_no == 1 then
        index_decoder = function (tuple)
            return box.unpack('i', tuple[0]), tuple[1]
        end
    end

    if conf == nil then
        conf = {}
    end

    resharding.cleanup(space_no, 0, 0, {
        key_decoder = function (key) return box.unpack('i', key) end,
        index_decoder = index_decoder,
        dryrun = is_dryrun,
        rows_per_sleep = conf.rows_per_sleep,
        sleep_time = conf.sleep_time,
    })
end
