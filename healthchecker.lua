-- This module should be used to check tarantool 1.6 (and older) availability.

local socket = require 'socket'
local json = require 'json'

-- healthcheck will be called any time when new connection is established
-- Connection will be closed when data will be sent
local function healthcheck(s)
	s:send(json.encode({
		read_only = box.cfg.read_only,
	}))
end

-- run_healthchecker is used to start healthcheck listener.
function run_healthchecker(host, port)
	if server ~= nil then
		server:close()
	end

	server = socket.tcp_server(host, port, healthcheck)
	if server == nil or server:error() ~= nil then
		str = "Can't start healthchecker"
		if server ~= nil then
			str = str + ": " + server:error()
		end
		box.error(box.error.PROC_LUA, str)
	end
end
