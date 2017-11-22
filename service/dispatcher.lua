local skynet = require "skynet"
local socket = require "socket"

local id , db = ...

id = tonumber(id)
db = tonumber(db)

local function mainloop()
	while true do
		local str = socket.readline(id,"\n")
		if str then
			local cmd, key = string.match(str, "(%w+)%s*(.*)")
			if cmd == "S" or cmd == "L" or cmd == "C" then
				skynet.send(db, "lua", cmd, key)
			elseif cmd == "V" then
				local ret = skynet.call(db, "lua", cmd, key)
				if ret then
					socket.write(id, tostring(ret))
				end
			else
				print("Unknown command", cmd, key)
			end
		else
			socket.close(id)
			skynet.exit()
			return
		end
	end
end

skynet.start(function()
	socket.start(id)
	skynet.fork(mainloop)
end)
