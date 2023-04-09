local string = string
local sql = sql

module( 'sqlt', package.seeall )

MetaTable = MetaTable or {}
MetaTable.__index = MetaTable

do

    local TYPE_VECTOR = TYPE_VECTOR
    local TYPE_NUMBER = TYPE_NUMBER
    local TYPE_ANGLE = TYPE_ANGLE
    local TYPE_COLOR = TYPE_COLOR
    local TYPE_TABLE = TYPE_TABLE
    local TYPE_BOOL = TYPE_BOOL

    do

        local util_JSONToTable = util.JSONToTable
        local tonumber = tonumber
        local Vector = Vector
        local Angle = Angle
        local Color = Color

        local dataUnpacking = {
            [TYPE_NUMBER] = function( value )
                return tonumber( value )
            end,
            [TYPE_BOOL] = function( value )
                return value == '1'
            end,
            [TYPE_TABLE] = function( value )
                return util_JSONToTable( value )
            end,
            [TYPE_COLOR] = function( value )
                local col = string.Split( value, 'c' )
                return Color( col[ 1 ], col[ 2 ], col[ 3 ], col[ 4 ] )
            end,
            [TYPE_VECTOR] = function( value )
                local vec = string.Split( value, 'v' )
                return Vector( vec[ 1 ], vec[ 2 ], vec[ 3 ] )
            end,
            [TYPE_ANGLE] = function( value )
                local ang = string.Split( value, 'a' )
                return Angle( ang[ 1 ], ang[ 2 ], ang[ 3 ] )
            end
        }

        function MetaTable:Unpack( str )
            local data = string.Split( str, ';' )
            if #data ~= 2 then return default end

            local func, value = dataUnpacking[ tonumber( data[ 1 ] ) ], data[ 2 ]
            if func ~= nil then
                value = func( value )
            end

            return value
        end

    end

    do

        local util_TableToJSON = util.TableToJSON
        local table_concat = table.concat
        local tostring = tostring
        local Either = Either
        local TypeID = TypeID

        local dataPacking = {
            [TYPE_BOOL] = function( value )
                return Either( value, '1', '0' )
            end,
            [TYPE_TABLE] = function( value )
                return util_TableToJSON( value )
            end,
            [TYPE_COLOR] = function( value )
                return table_concat( value, 'c' )
            end,
            [TYPE_VECTOR] = function( value )
                return string.format( '%sv%sv%s', value[ 1 ], value[ 2 ], value[ 3 ] )
            end,
            [TYPE_ANGLE] = function( value )
                return string.format( '%sa%sa%s', value[ 1 ], value[ 2 ], value[ 3 ] )
            end
        }

        function MetaTable:Pack( value )
            local valueType = TypeID( value )
            local func = dataPacking[ valueType ]
            if func ~= nil then
                value = func( value )
            else
                value = tostring( value )
            end

            return valueType .. ';' .. value
        end

    end

end

function MetaTable:Get( key, default )
    key = sql.SQLStr( key )

    local queueValue = self.Queue[ key ]
    if queueValue ~= nil then
        return self:Unpack( string.sub( queueValue, 2, #queueValue - 1) )
    end

    local query = sql.Query( string.format( 'SELECT `value` FROM %s WHERE `key` = %s;', self.Name, key ) )
    if not query then return default end

    local result = query[ 1 ]
    if not result then return default end

    local value = result.value
    if not value then return default end

    return self:Unpack( value )
end

function MetaTable:Set( key, value )
    ArgAssert( key, 1, 'string' )
    if string.Trim( key ) == '' then return error( 'invalid key' ) end
    self.Queue[ sql.SQLStr( key ) ] = sql.SQLStr( self:Pack( value ) )
    self:Sync()
end

do

    local timer_Create = timer.Create
    local pairs = pairs

    function MetaTable:Sync()
        timer_Create( self.Name, 0.025, 1, function()
            sql.Query( 'BEGIN;' )

            for key, value in pairs( self.Queue ) do
                sql.Query( string.format( 'INSERT OR REPLACE INTO %s ( `key`, `value` ) VALUES ( %s, %s );', self.Name, key, value ) )
                self.Queue[ key ] = nil
            end

            sql.Query( 'COMMIT;' )
        end)
    end

end

function MetaTable:Create()
    sql.Query( 'CREATE TABLE IF NOT EXISTS ' .. self.Name .. ' ( `key` TEXT NOT NULL PRIMARY KEY, `value` TEXT NOT NULL);' )
end

function MetaTable:Drop()
    sql.Query( 'DROP TABLE ' .. self.Name .. ';' )
end

function MetaTable:Recreate()
    self:Drop()
    self:Create()
end

function Create( name )
    local new = setmetatable( {
        ['Name'] = sql.SQLStr( 'sqlt_' .. name ),
        ['Queue'] = {}
    }, MetaTable )

    new:Create()
    return new
end