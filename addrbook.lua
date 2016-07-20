--
-- addrbook.lua
--

-- Space 0: User recipients info
--   Tuple: { user_id (NUM), data (STR) }
--   Index 0: HASH user_id

-- Space 1: User recipients info
--   Tuple: { user_id (NUM), rcp_email (STR), rcp_name (NUM), timestamp (NUM), weight (NUM) }
--   Index 0: TREE { user_id, rcp_email }

-- configuration:
-- space[0].enabled = true
-- space[0].index[0].type = "HASH"
-- space[0].index[0].unique = true
-- space[0].index[0].key_field[0].fieldno = 0
-- space[0].index[0].key_field[0].type = "NUM"
--
-- space[1].enabled = true
-- space[1].index[0].type = "TREE"
-- space[1].index[0].unique = true
-- space[1].index[0].key_field[0].fieldno = 0
-- space[1].index[0].key_field[0].type = "NUM"
-- space[1].index[0].key_field[1].fieldno = 1
-- space[1].index[0].key_field[1].type = "STR"

addrbook_log_ab_creations = false
MAX_SHARD_INDEX = 1024

if perl_crc32 == nil then
	if require == nil then
		error("Add this module into init.lua or preload perl_crc32 before module invoke")
	else
		require('perl_crc32')
	end
end

local function calculate_shard_number(key)
	if perl_crc32 == nil then
		return -1
	end

	local crc32 = perl_crc32(key)
	if crc32 == nil then
		error("Unexpected return from perl_crc32 for key " .. key .. " (nil)")
	end

	return crc32 % MAX_SHARD_INDEX
end

function addrbook_add_recipient(user_id, rcp_email, rcp_name, timestamp)
	user_id = box.unpack('i', user_id)
	timestamp = box.unpack('i', timestamp)

	local t = box.select_limit(1, 0, 0, 1, user_id, rcp_email)
	if t == nil then
		box.insert(1, user_id, rcp_email, rcp_name, timestamp, 1)
		return 1 -- new contact inserted
	end
	if box.unpack('i', t[3]) < timestamp then
		box.update(1, { user_id, rcp_email }, "=p=p+p", 2, rcp_name, 3, timestamp, 4, 1)
		return 2 -- contact updated
	end

	box.update(1, { user_id, rcp_email }, "+p", 4, 1)
	return 0 -- contact weight updated only
end

function addrbook_get_recipients(user_id)
	if user_id == nil then
		return nil
	end

	user_id = box.unpack('i', user_id)
	return box.select(1, 0, user_id)
end

function addrbook_get(user_id)
	if user_id == nil then
		return nil
	end
	user_id = box.unpack('i', user_id)
	return box.select_limit(0, 0, 0, 1, user_id)
end

function addrbook_put(user_id, book)
	if user_id == nil or book == nil then
		return nil
	end
	user_id = box.unpack('i', user_id)

	-- XXX: there is no race condition between select/insert and select/update
	-- because of implementation details of tarantool 1.5 (only modificating methods yield). May change in newer versions of tarantool.
	local t = box.select_limit(0, 0, 0, 1, user_id)
	if t == nil then
		if addrbook_log_ab_creations then
			print("Trying to create AB for user " .. user_id .. ", shard == " .. calculate_shard_number(user_id) .. " via addrbook_put")
		end
		return box.insert(0, user_id, book)
	end

	return box.update(0, user_id, "=p", 1, book)
end

function addrbook_delete(user_id)
	if user_id == nil then
		return nil
	end
	user_id = box.unpack('i', user_id)

	if addrbook_log_ab_creations then
		print("Trying to delete AB for user " .. user_id .. ", shard == " .. calculate_shard_number(user_id))
	end

	return box.delete(0, user_id)
end
