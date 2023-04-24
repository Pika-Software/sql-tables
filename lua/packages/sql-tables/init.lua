local spon = import( "https://raw.githubusercontent.com/thelastpenguin/sPON/master/lua/includes/modules/spon.lua" )
local string = string
local sql = sql

module( "sqlt", package.seeall )

MetaTable = MetaTable or {}
MetaTable.__index = MetaTable

function MetaTable:Get( key, default )
    key = sql.SQLStr( key )

    local queueValue = self.Queue[ key ]
    if queueValue ~= nil then
        return spon.decode( queueValue )
    end

    local query = sql.Query( string.format( "SELECT `value` FROM %s WHERE `key` = %s;", self.Name, key ) )
    if not query then return default end

    local result = query[ 1 ]
    if not result then return default end

    local value = result.value
    if not value then return default end

    return spon.decode( value )
end

function MetaTable:Set( key, value )
    ArgAssert( key, 1, "string" )
    if string.Trim( key ) == "" then return error( "invalid key" ) end
    self.Queue[ sql.SQLStr( key ) ] = spon.encode( value )
    self:Sync()
end

do

    local timer_Create = timer.Create
    local pairs = pairs

    function MetaTable:Sync()
        timer_Create( self.Name, 0.025, 1, function()
            sql.Query( "BEGIN;" )

            for key, value in pairs( self.Queue ) do
                sql.Query( string.format( "INSERT OR REPLACE INTO %s ( `key`, `value` ) VALUES ( %s, %s );", self.Name, key, sql.SQLStr( value ) ) )
                self.Queue[ key ] = nil
            end

            sql.Query( "COMMIT;" )
        end )
    end

end

function MetaTable:Create()
    sql.Query( "CREATE TABLE IF NOT EXISTS " .. self.Name .. " ( `key` TEXT NOT NULL PRIMARY KEY, `value` TEXT NOT NULL);" )
end

function MetaTable:Drop()
    sql.Query( "DROP TABLE " .. self.Name .. ";" )
end

function MetaTable:Recreate()
    self:Drop()
    self:Create()
end

function Create( name )
    local new = setmetatable( {
        ["Name"] = sql.SQLStr( "sqlt_" .. name ),
        ["Queue"] = {}
    }, MetaTable )

    new:Create()
    return new
end