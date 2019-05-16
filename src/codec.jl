module Codecs

using Libz
import Snappy

export Codec

struct Codec{Name}
end

function create(name::String)
    if name == "deflate"
        Codec{:deflate}()
    elseif name == "null"
        Codec{:null}()
    elseif name == "snappy"
        Codec{:snappy}()
    end
end

name(::Codec{:null}) = "null"
name(::Codec{:deflate}) = "deflate"
name(::Codec{:snappy}) = "snappy"

compress(::Codec{:null}, data) = data
decompress(::Codec{:null}, data) = data

function compress(::Codec{:deflate}, data)
    stream = Libz.ZlibDeflateOutputStream(data; raw = true, gzip = false)
    read(stream)
end

function decompress(::Codec{:deflate}, data)
    stream = Libz.ZlibInflateInputStream(data; raw = true, gzip = false)
    read(stream)
end

compress(::Codec{:snappy}, data) = Snappy.compress(data)
decompress(::Codec{:snappy}, data) = Snappy.uncompress(data)

end
