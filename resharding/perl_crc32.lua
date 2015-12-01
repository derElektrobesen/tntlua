if ffi == nil then
    if require == nil then
        error('You must restart tarantool')
    end
    ffi = require('ffi')
end

ffi.cdef[[
    unsigned perl_crc32(const char *key, unsigned len);
]]

local libperlcrc32 = ffi.load("perlcrc32")

function perl_crc32(key)
    local _key, _key_len
    if type(key) == "number" then
        _key = box.pack('i', key)
        _key_len = 4
    elseif type(key) == "string" then
        _key = key
        _key_len = #key
    else
        error("String or number key is expected in perl_crc32: '" .. key .. "'")
    end

    return libperlcrc32.perl_crc32(_key, _key_len)
end
