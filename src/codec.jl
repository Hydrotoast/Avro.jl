module Codecs

using Libz

export Codec

immutable Codec{Name}
end

function create(name::String)
    if name == "deflate"
        Codec{:deflate}()
    elseif name == "null"
        Codec{:null}()
    end
end

name(::Codec{:null}) = "null"
name(::Codec{:deflate}) = "deflate"

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

end
