local encoder = install( "packages/glua-encoder.lua", "https://raw.githubusercontent.com/Pika-Software/glua-encoder/main/lua/packages/glua-encoder.lua" )
local string = string
local sql = sql

local timer_Create = timer.Create
local setmetatable = setmetatable
local ArgAssert = ArgAssert
local pairs = pairs
local type = type

module( "sqlt" )

local meta = MetaTable
if type( meta ) ~= "table" then
    meta = {}; MetaTable = meta
end

meta.__index = meta

function meta:Get( key, default )
    ArgAssert( key, 1, "string" )

    local queueValue = self.Queue[ key ]
    if queueValue ~= nil then
        return encoder.Decode( queueValue )
    end

    local query = sql.Query( string.format( "select `Value` from %s where `Key` = %s;", self.Name, sql.SQLStr( key ) ) )
    if type( query ) ~= "table" then
        return default
    end

    local result = query[ 1 ]
    if type( result ) ~= "table" then
        return default
    end

    local value = result.Value
    if type( value ) ~= "string" then
        return default
    end

    return encoder.Decode( value )
end

function meta:Set( key, value, instant )
    ArgAssert( key, 1, "string" )

    self.Queue[ key ] = encoder.Encode( value )

    if instant then
        return self:Sync()
    end

    timer_Create( self.Name, 0.025, 1, function()
        self:Sync()
    end )

    return self
end

function meta:Sync()
    sql.Query( "begin;" )

    for key, value in pairs( self.Queue ) do
        sql.Query( string.format( "insert or replace into %s ( `Key`, `Value` ) values ( %s, %s );", self.Name, sql.SQLStr( key ), sql.SQLStr( value ) ) )
        self.Queue[ key ] = nil
    end

    sql.Query( "commit;" )
    return self
end

function meta:Create()
    sql.Query( string.format( "create table if not exists %s ( `Key` text not null primary key, `Value` text not null );", self.Name ) )
    return self
end

function meta:Drop()
    sql.Query( string.format( "drop table %s;", self.Name ) )
    return self
end

function meta:Reset()
    self:Drop()
    self:Create()
    return self
end

function Create( name )
    ArgAssert( name, 1, "string" )
    return setmetatable( {
        ["Name"] = sql.SQLStr( "sqlt_" .. name ),
        ["Queue"] = {}
    }, meta ):Create()
end