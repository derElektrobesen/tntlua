--
-- rima.lua
--

--
-- Task manager for imap collector.
-- Task's key is a user email address.
-- Rima can manage some tasks with the same key.
-- Tasks with identical keys will be groupped and managed as one bunch of tasks.
--
-- Producers can adds tasks by rima_put() calls.
-- Consumer request a bunch of tasks (with common key) by calling rima_get().
-- When Rima gives a task to worker it locks the key until worker calls rima_done(key).
-- Rima does not return task with already locked keys.
--

--
-- Space 0: Remote IMAP Collector Task Queue
--   Tuple: { task_id (NUM64), key (STR), task_description (NUM), add_time (NUM) }
--   Index 0: TREE { task_id }
--   Index 1: TREE { key, task_id }
--
-- Space 2: Task Priority
--   Tuple: { key (STR), priority (NUM), is_locked (NUM), lock_time (NUM), lock_source (STR) }
--   Index 0: TREE { key }
--   Index 1: TREE { priority, is_locked, lock_time }
--
-- Space 3: Mail Fetcher Queue (Special queue for fast single message loading)
--   Tuple: { task_id (NUM64), key (STR), task_description (NUM), add_time (NUM) }
--   Index 0: TREE { task_id }
--   Index 1: TREE { key }
--
-- Space 4: Fast tasks monitoring
--   Tuple: { key (NUM), added_tasks_count (NUM), deleted_tasks_count (NUM) }
--   Index 0: TREE { key }
--

local EXPIRATION_TIME = 30 * 60 -- seconds
local FAST_TASKS_STAT_EXP_TIME = 60 * 60 * 24 * 365 -- seconds (1 year)

--
-- Put task to the queue.
--
local function rima_put_impl(key, data, prio, ts)
	-- insert task data into the queue
	box.auto_increment(0, key, data, ts)
	-- increase priority of the key
	local pr = box.select_limit(2, 0, 0, 1, key)
	if pr == nil then
		box.insert(2, key, prio, 0, box.time())
	elseif box.unpack('i', pr[1]) < prio then
		box.update(2, key, "=p", 1, prio)
	end
	return 1
end

function rima_put(key, data) -- deprecated
	rima_put_impl(key, data, 512, box.time())
end

function rima_put_with_prio(key, data, prio)
	prio = box.unpack('i', prio)

	rima_put_impl(key, data, prio, box.time())
end

function rima_put_with_prio_and_ts(key, data, prio, ts)
	prio = box.unpack('i', prio)
	ts = box.unpack('i', ts)

	rima_put_impl(key, data, prio, ts)
end

function rima_put_sync(key, data, prio)
	prio = box.unpack('i', prio)

	return rima_put_impl(key, data, prio, box.time())
end

--
-- Get a key for fast tasks monitoring
--
local function rima_get_monitoring_key(t)
	if t == nil or t == 0 then
		t = box.time()
	end
	return t - (t % 60)
end

--
-- Increment fast tasks count
--
local function rima_inc_fast_tasks_count()
	local key = rima_get_monitoring_key()
	local update_result = box.update(4, key, '+p', 1, 1)

	if update_result == nil then
		-- no stat for current key
		box.insert(4, key, 1, 0)
	end

	-- increment total number of fast tasks in queue
	update_result = box.update(4, 0, '+p', 1, 1)
	if update_result == nil then
		box.insert(4, 0, 1)
	end
end

--
-- Decrement fast tasks count
--
local function rima_dec_fast_tasks_count()
	local key = rima_get_monitoring_key()
	local update_result = box.update(4, key, '+p', 2, 1)

	if update_result == nil then
		-- no stat for current key
		box.insert(4, key, 0, 1)
	end

	-- decrement total number of fast tasks
	box.update(4, 0, '-p', 1, 1) -- ignore case when no tasks was in queue
end

--
-- Remove old records from tarantool
--
local function rima_clear_expired_stat()
	local cur_key = rima_get_monitoring_key(box.time())
	local expired_key = cur_key - FAST_TASKS_STAT_EXP_TIME
	local iter = box.space[4].index[0]:iterator(box.index.LE, expired_key)

	for tuple in iter do
		box.delete(4, tuple[0])
	end
end

--
-- Get statistic from monitoring for given time interval
--
function rima_get_monitoring_statistics(start_time, end_time)
	if start_time ~= nil then
		start_time = box.unpack('i', start_time)
	end
	if end_time ~= nil then
		end_time = box.unpack('i', end_time)
	end

	rima_clear_expired_stat()

	if start_time == nil or start_time == 0 then
		-- request all statistics, but 0 is global tasks counter
		start_time = 1
	else
		start_time = rima_get_monitoring_key(start_time)
	end

	end_time = rima_get_monitoring_key(end_time)
	if end_time < start_time then
		end_time, start_time = start_time, end_time
	end

	local tuple = box.select(4, 0, 0)
	local currently_in_queue = 0
	if tuple ~= nil then
		currently_in_queue = box.unpack('i', tuple[1])
	end

	local total_added, total_deleted = 0, 0
	local iter = box.space[4].index[0]:iterator(box.index.GE, start_time)
	for tuple in iter do
		if box.unpack('i', tuple[0]) > end_time then
			break
		end

		total_added = total_added + box.unpack('i', tuple[1])
		total_deleted = total_deleted + box.unpack('i', tuple[2])
	end

	-- string will be returned
	return box.cjson.encode({
		fast_tasks_in_queue = currently_in_queue,
		fast_tasks_added = total_added,
		fast_tasks_deleted = total_deleted,
	})
end

--
-- Put fetch single mail task to the queue.
--
function rima_put_fetchmail(key, data)
	box.auto_increment(3, key, data, box.time())
	rima_inc_fast_tasks_count()
end

local function get_prio_key(prio, source)
	local v = box.select_limit(2, 1, 0, 1, prio, 0)
	if v == nil then return nil end

	if source == nil then source = "" end

	-- lock the key
	local key = v[0]
	box.update(2, key, "=p=p=p", 2, 1, 3, box.time(), 4, source)

	return key
end

local function get_key_data(key)
	local result = { key }

	local tuples = { box.select_limit(0, 1, 0, 1000, key) }
	for _, tuple in pairs(tuples) do
		tuple = box.delete(0, box.unpack('l', tuple[0]))
		if tuple ~= nil then
			table.insert(result, { box.unpack('i', tuple[3]), tuple[2] } )
		end
	end

	return result
end

--
-- Request tasks from the queue.
--
function rima_get_ex(prio, source)
	prio = box.unpack('i', prio)

	local key = get_prio_key(prio, source)
	if key == nil then return end
	return unpack(get_key_data(key))
end

--
-- Request fetch single mail tasks from the queue.
--
function rima_get_fetchmail()
	local tuple = box.select_range(3, 0, 1)
	if tuple == nil then return end

	local key = tuple[1]

	local result = {}
	local n = 0

	local tuples = { box.select_limit(3, 1, 0, 1000, key) }
	for _, tuple in pairs(tuples) do
		tuple = box.delete(3, box.unpack('l', tuple[0]))
		rima_dec_fast_tasks_count()
		if tuple ~= nil then
			table.insert(result, { box.unpack('i', tuple[3]), tuple[2] })
			n = 1
		end
	end

	if n == 0 then return end
	return key, unpack(result)
end

--
-- Request tasks from the queue for concrete user.
--
function rima_get_user_tasks(key, source)
	local lock_acquired = rima_lock(key, source)
	if lock_acquired == 0 then
		local pr = box.select_limit(2, 0, 0, 1, key)
		if pr[4] ~= source and source ~= "force_run" then return end
		lock_acquired = 1
	end

	return unpack(get_key_data(key))
end

--
-- Notify manager that tasks for that key was completed.
-- Rima unlocks key and next rima_get() may returns tasks with such key.
-- In case of non-zero @unlock_delay user unlock is defered for @unlock_delay seconds (at least).
--
function rima_done(key, unlock_delay)
	if unlock_delay ~= nil then unlock_delay = box.unpack('i', unlock_delay) end

	local pr = box.select_limit(2, 0, 0, 1, key)
	if pr == nil then return end

	if unlock_delay ~= nil and unlock_delay > 0 then
		box.update(2, key, "=p=p", 2, 1, 3, box.time() - EXPIRATION_TIME + unlock_delay)
	elseif box.select_limit(0, 1, 0, 1, key) == nil then
		-- no tasks for this key in the queue
		box.delete(2, key)
	else
		box.update(2, key, "=p=p", 2, 0, 3, box.time())
	end
end

--
-- Explicitly lock tasks for the key.
--
function rima_lock(key, source)
	local pr = box.select_limit(2, 0, 0, 1, key)
	if pr ~= nil and box.unpack('i', pr[2]) > 0 then return 0 end

	if source == nil then source = "" end

	-- lock the key
	if pr ~= nil then
		box.update(2, key, "=p=p=p", 2, 1, 3, box.time(), 4, source)
	else
		box.insert(2, key, 0, 1, box.time(), source)
	end

	return 1
end

--
-- Delete info and all tasks for user
--

function rima_delete_user(email)
	local something_deleted = 0
	repeat
		something_deleted = 0

		local tuple = box.delete(2, email)
		if tuple ~= nil then something_deleted = 1 end

		local tuples = { box.select_limit(3, 1, 0, 1000, email) }
		for _, tuple in pairs(tuples) do
			tuple = box.delete(3, box.unpack('l', tuple[0]))
			something_deleted = 1
		end

		tuples = { box.select_limit(0, 1, 0, 1000, email) }
		for _, tuple in pairs(tuples) do
			tuple = box.delete(0, box.unpack('l', tuple[0]))
			something_deleted = 1
		end
	until something_deleted == 0
end

--
-- Run expiration of tuples
--

local function is_expired(args, tuple)
	if tuple == nil or #tuple <= args.fieldno then
		return nil
	end

	-- expire only locked keys
	if box.unpack('i', tuple[2]) == 0 then return false end

	local field = tuple[args.fieldno]
	local current_time = box.time()
	local tuple_expire_time = box.unpack('i', field) + args.expiration_time
	return current_time >= tuple_expire_time
end

local function delete_expired(spaceno, args, tuple)
	rima_done(tuple[0])
end

dofile('expirationd.lua')

expirationd.run_task('expire_locks', 2, is_expired, delete_expired, {fieldno = 3, expiration_time = EXPIRATION_TIME})
