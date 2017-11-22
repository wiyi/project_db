local skynet = require "skynet"
local socket = require "socket"

skynet.start(function()
	skynet.error("main---->>start")
		
	local listen_addr = skynet.getenv "listen"
	local addr, port = string.match(listen_addr, "([^:%s]+)%s*:%s*([^:%s]+)")
	skynet.error(string.format("Listen on %s:%d", addr, port))

	skynet.start(function()
		local db = skynet.newservice("dbserver")
		local id = socket.listen(addr, port)
		socket.start(id , function(id, addr)
			-- you can also call skynet.newservice for this socket id
			skynet.newservice("dispatcher", id, db)
		end)
	end)

    skynet.error("main---->>end")
    skynet.exit()
end)
