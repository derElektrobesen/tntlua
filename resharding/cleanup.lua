-- perl_crc32.lua module exports perl_crc32() function
if perl_crc32 == nil then
    if require == nil then
        error("Add this module into init.lua or preload perl_crc32 before module invoke")
    else
        require('perl_crc32')
    end
end

local MAX_SHARD_INDEX = 1024        -- constant from capron

local function calculate_shard_number(key)
    local crc32 = perl_crc32(key)
    if crc32 == nil then
        error("Unexpected return from perl_crc32 for key " .. key .. " (nil)")
    end

    return crc32 % MAX_SHARD_INDEX
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
        if shard_no < opts.first_index or shard_no >= opts.last_index then
            -- Key should be stored on remote shard => delete it
            rows_removed = rows_removed + 1
            if opts.verbose then
                print("Trying to remove tuple with key " .. key .. " (hash_func == " .. shard_no .. ")")
            end

            if opts.dryrun == false then
                box.delete(space_no, opts.index_decoder(row))
            end
        end
    end

    return rows_removed
end

local cleanup_shard_fiber = nil

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

    cleanup_shard_fiber = nil

    local function msg(x)
        print(">> RESULTS: " .. x)
    end

    msg("Space " .. space_no .. ", index " .. index_no .. " was cleanuped successfully. Results:")
    msg(n_rows .. " rows processed (" .. iter_no .. " iterations)")
    msg(rows_removed_total .. " rows removed")
    msg((n_rows - rows_removed_total) .. " rows skipped")
    msg(box.space[space_no]:len() .. " rows left in tarantool")

    if opts.on_done then
        opts.on_done()
    end
end

--
-- Function removes unnecessary records from current tarantool
-- @opts is a table with following options:
--      first_index, last_index
--          this values will be used to understand what rows should be SAVED
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
local function cleanup_tarantool(space, index, key_field_no, opts)
    if opts.first_index == nil or opts.last_index == nil then
        error("Indexes 'first_index' && 'last_index' are required in conf!")
    end

    if cleanup_shard_fiber ~= nil and box.fiber.status(cleanup_shard_fiber) ~= 'dead' then
        error("Can't start cleanup_shard: fiber is already running, status: " .. box.fiber.status(cleanup_shard_fiber)
            .. ", id: " .. box.fiber.id(cleanup_shard_fiber))
    end

    local _opts = {
        dryrun = true,
        rows_per_sleep = 10000,
        sleep_time = 0.1,
        index_decoder = function (tuple) return tuple[key_field_no] end,
        first_index = opts.first_index,
        last_index = opts.last_index,
        verbose = opts.verbose,
    }

    if space == nil or type(space) ~= 'number' or space < 0
            or index == nil or type(index) ~= 'number' or index < 0
            or key_field_no == nil or type(key_field_no) ~= 'number' or key_field_no < 0 then
        error("Invalid arguments!")
    end

    for _, v in ipairs({ 'key_decoder', 'index_decoder', 'on_done' }) do
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

    cleanup_shard_fiber = box.fiber.wrap(function ()
        box.fiber.sleep(1) -- to remove race condition
        run_cleanup_nocheck(space, index, key_field_no, _opts)
    end)

    cleanup_shard_fiber:name("cleanup_shard_fiber_space_" .. space)
    print("Started fiber with id " .. cleanup_shard_fiber:id()
        .. ", name == " .. cleanup_shard_fiber:name())
end

-- Function removes unnecessary records from current tarantool
-- @opts is a table with following options:
--      rows_per_sleep
--          number of rows to process before sleep (10_000 by default)
--      sleep_time
--          in seconds (0.1 sec by default)
--      dryrun
--          use 'dryrun = false' to disable dryrun mode. (enabled by default)
--
-- HOW TO find indexes for this function:
-- +-------+-------------------+-------------------+
-- | db_id | db_addr           | db_rep_addr       |
-- +-------+-------------------+-------------------+
-- |   256 | 5.61.232.50:30072 | 5.61.232.50:30072 |     ===> first_index == 0, last_index == 256
-- |   512 | 5.61.232.50:30073 | 5.61.232.50:30073 |     ===> first_index == 256, last_index == 512
-- |   768 | 5.61.232.50:30074 | 5.61.232.50:30074 |     ===> first_index == 512, last_index == 768
-- |  1024 | 5.61.232.50:30071 | 5.61.232.50:30071 |     ===> first_index == 768, last_index == 1024
-- +-------+-------------------+-------------------+
--
function ab_cleanup(first_index, last_index, opts)
    if not opts then
        opts = {}
    end

    local conf = {
        first_index = first_index,
        last_index = last_index,
        rows_per_sleep = opts.rows_per_sleep,
        sleep_time = opts.sleep_time,
        dryrun = opts.dryrun,
        key_decoder = function (key) return box.unpack('i', key) end,
        index_decoder = function (tuple) return box.unpack('i', tuple[0]) end,
        verbose = opts.verbose == nil and true or opts.verbose,
    }

    conf.on_done = function ()
        conf.on_done = nil
        conf.index_decoder = function (tuple) return box.unpack('i', tuple[0]), tuple[1] end

        cleanup_tarantool(1, 0, 0, conf)
    end

    cleanup_tarantool(0, 0, 0, conf)
end
