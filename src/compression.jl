export compression, compress, decompress, NullCompression, DeflateCompression,
    SnappyCompression

    
module NullCompression

const name = :null
compress(data) = data
decompress(data) = data

end  # module NullCompression


module DeflateCompression

import Libz

const name = :deflate

function compress(data)
    stream = Libz.ZlibDeflateOutputStream(data; raw = true, gzip = false)
    read(stream)
end

function decompress(data)
    stream = Libz.ZlibInflateInputStream(data; raw = true, gzip = false)
    read(stream)
end

end  # module DeflateCompression


module SnappyCompression

import Snappy

const name = :snappy
compress(data) = Snappy.compress(data)
decompress(data) = Snappy.uncompress(data)

end  # module SnappyCompression


const COMPRESSIONS = Dict{Symbol, Module}(
    :null => NullCompression,
    :deflate => DeflateCompression,
    :snappy => SnappyCompression
)

compression(sym::Symbol) = COMPRESSIONS[sym]

compress(m::Module, data) = m.compress(data)
decompress(m::Module, data) = m.decompress(data)

compress(sym::Symbol, data) = compress(compression(sym), data)
decompress(sym::Symbol, data) = decompress(compression(sym), data)
