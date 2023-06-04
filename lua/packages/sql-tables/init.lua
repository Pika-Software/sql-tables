
local encoder = install( "packages/glua-encoder", "https://github.com/Pika-Software/glua-encoder" )
local string = string
local sql = sql

local timer_Create = timer.Create
local setmetatable = setmetatable
local pairs = pairs
local error = error
local type = type

module( "sqlt" )

local meta = MetaTable
if type( meta ) ~= "table" then
    meta = {}; MetaTable = meta
end

meta.__index = meta

function meta:Get( key, default )
    key = sql.SQLStr( key )

    local queueValue = self.Queue[ key ]
    if queueValue ~= nil then
        return encoder.Decode( queueValue )
    end

    local query = sql.Query( string.format( "SELECT `value` FROM %s WHERE `key` = %s;", self.Name, key ) )
    if not query then return default end

    local result = query[ 1 ]
    if not result then return default end

    local value = result.value
    if not value then return default end

    return encoder.Decode( value )
end

function meta:Set( key, value )
    if string.Trim( key ) == "" then return error( "invalid key" ) end
    self.Queue[ sql.SQLStr( key ) ] = encoder.Encode( value )
    self:Sync()
    return self
end

function meta:Sync()
    timer_Create( self.Name, 0.025, 1, function()
        sql.Query( "BEGIN;" )

        for key, value in pairs( self.Queue ) do
            sql.Query( string.format( "INSERT OR REPLACE INTO %s ( `key`, `value` ) VALUES ( %s, %s );", self.Name, key, sql.SQLStr( value ) ) )
            self.Queue[ key ] = nil
        end

        sql.Query( "COMMIT;" )
    end )

    return self
end

function meta:Create()
    sql.Query( "CREATE TABLE IF NOT EXISTS " .. self.Name .. " ( `key` TEXT NOT NULL PRIMARY KEY, `value` TEXT NOT NULL);" )
    return self
end

function meta:Drop()
    sql.Query( "DROP TABLE " .. self.Name .. ";" )
    return self
end

function meta:Reset()
    self:Drop()
    self:Create()
    return self
end

function Create( name )
    return setmetatable( {
        ["Name"] = sql.SQLStr( "sqlt_" .. name ),
        ["Queue"] = {}
    }, meta ):Create()
end
