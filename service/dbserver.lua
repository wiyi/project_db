local skynet = require "skynet"
local socket = require "socket"
local mysql = require "skynet.db.mysql"

local fd = nil	-- redis socket fd

local function on_connect(db)
    db:query("set charset utf8");
end
local mysql_db=mysql.connect({
    host="127.0.0.1",
    port=3306,
    database="skynet",
    user="root",
    password="1",
    max_packet_size = 1024 * 1024,
    on_connect = on_connect
})

-- GC?
mysql_db_gc = setmetatable({} , { __gc = function() mysql_db.disconnect() end })

local redis_addr = skynet.getenv "redisaddr"
local addr, port, db = string.match(redis_addr, "([^:%s]+)%s*:%s*([^:%s%[]+)%s*%[%s*(%d+)%]")
port = tonumber(port)
db = tostring(db)
skynet.error(string.format("Redis %s : %d select(%d)", addr, port, db))


local dbcmd = { head = 1, tail = 1 }

local cache = {}

local function push(v)
	dbcmd[dbcmd.tail] = v
	dbcmd.tail = dbcmd.tail + 1
end

local function pop()
	if dbcmd.head == dbcmd.tail then
		return
	end
	local v = dbcmd[dbcmd.head]
	dbcmd[dbcmd.head] = nil
	dbcmd.head = dbcmd.head + 1
	if dbcmd.head == dbcmd.tail then
		dbcmd.head = 1
		dbcmd.tail = 1
	end
	return v
end

--CMD--
local CMD = {}
function CMD.S(key)--save?
    local load_command = string.format("*2\r\n$7\r\nHGETALL\r\n$%d\r\n%s\r\n",#key,key)
    skynet.error("CMD.S-->\n"..load_command)
end

function CMD.L(key)--load?
    skynet.error("CMD.L-->"..key)
end

function CMD.C()--commit?
    skynet.error("CMD.C-->"..key)
end

function CMD.D(key)--delete?
    skynet.error("CMD.D-->"..key)
end

function CMD.V(key)
    skynet.error("CMD.V-->"..key)
end

local dispatcher

local function connect_redis(addr, port, db)
	fd = socket.open(addr, port)
    if fd then
        local str_ = string.format("*2\r\n$6\r\nSELECT\r\n$%d\r\n%d\r\n",#db,db)
        skynet.error("connect_redis-->\n"..str_)
        socket.write(fd, str_)
		local ok = readline()
		assert(ok == "+OK", string.format("Select %d failed", db))
		for i = dbcmd.head, dbcmd.tail -1 do
			socket.write(fd, dbcmd[i])
		end
		skynet.error("connect ok")
		skynet.fork(dispatcher)
		return true
	end
end

local function dispatch_one()
	local firstline = readline()
	if firstline == "+OK" then
		pop()
	else
		local r,data = read_response(firstline)
		if type(r) == "number" and r > 0 then
			-- save key
			local cmd = pop()
            local key = string.match(cmd,"\r\n([^%s]+)\r\n$")
            skynet.error("dispatch_one-->\n"..string.format("*%d\r\n$5\r\nHMSET\r\n$%d\r\n%s\r\n%s", r+2, #key, key, data))
		else
			print("error:", r, data)
			pop()
		end
	end
end

-- local function
function dispatcher()
	while true do
		local ok , err = pcall(dispatch_one)
		if not ok then
			-- reconnect
			skynet.error("redis disconnected:" , err)
			local ok, err = pcall(connect_redis, addr, port, db)
			if not ok then
				fd = nil
				skynet.error("Connect redis error: " ..  tostring(err))
				skynet.sleep(1000)
			end
			return
		end
	end
end

--开始连接--
skynet.start(function()
	assert(connect_redis(addr,port,db) , "Connect failed")
	skynet.dispatch("lua", function(session,addr, cmd, ...)
		CMD[cmd](...)
	end)
end)