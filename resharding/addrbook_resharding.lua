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
        _G[v] = resharding.wrap(v .. '_old', addrbook_get_uid)
    end

    print("Addrbook resharding enabled!")
end

-- Restore old addrbook handlers
function addrbook_disable_resharding()
    for _, v in ipairs(functions_names) do
        if _G[v .. '_old'] == nil then
            error('Can\'t restore shard configuration! Resharding is disabled or handlers are corrupted. Restart tarantool in this case')
        end
    end

    for _, v in ipairs(functions_names) do
        _G[v] = _G[v .. '_old']
        _G[v .. '_old'] = nil
    end

    print("Addrbook resharding disabled!")
end

-- manually pass @is_dryrun == false to disable dryrun mode
function addrbook_cleanup_shard(space_no, is_dryrun)
    local index_decoder = (
        (space_no == 1)
            and function (tuple)
                return tuple[0], tuple[1]
            end
            or nil
        )

    resharding.cleanup(space_no, 0, 0, {
        key_decoder = function (key) return box.unpack('i', key) end,
        index_decoder = index_decoder,
        dryrun = is_dryrun,
    })
end
