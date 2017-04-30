module Io

import Base.write

using Avro.Common
using Avro.Schemas

export Encoder,
       Decoder,
       BinaryEncoder,
       BinaryDecoder,
       DatumWriter,
       DatumReader,
       encodeNull,
       encodeBoolean,
       encodeInt,
       encodeLong,
       encodeFloat,
       encodeDouble,
       encodeByte,
       encodeBytes,
       encodeString,
       decodeNull,
       decodeBoolean,
       decodeInt,
       decodeLong,
       decodeFloat,
       decodeDouble,
       decodeByte,
       decodeBytes,
       decodeString

abstract Encoder
abstract Decoder

"""
Writes binary data of builtin and primtive types to an output stream.
"""
immutable BinaryEncoder <: Encoder
    stream::IO
end

"""
Reads data of simple and primitive types from an input stream.
"""
immutable BinaryDecoder <: Decoder
    stream::IO
end

# Encoders

encodeNull(encoder::BinaryEncoder, value::Void) = 0
encodeBoolean(encoder::BinaryEncoder, value::Bool) = write(encoder.stream, value)

function encodeInt(encoder::BinaryEncoder, value::Int32)
    stream = encoder.stream
    bytes_written = 0
    n = (value << 1) $ (value >> 31)
    if n > 0x7F
        bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
        n >>>= 7
        if n > 0x7F
            bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
            n >>>= 7
            if n > 0x7F
                bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
                n >>>= 7
                if n > 0x7F
                    bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
                    n >>>= 7
                end
            end
        end
    end
    bytes_written += write(stream, n % UInt8)
    bytes_written
end

function encodeLong(encoder::BinaryEncoder, value::Int64)
    stream = encoder.stream
    bytes_written = 0
    n = (value << 1) $ (value >> 63)
    if n > 0x7F
        bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
        n >>>= 7
        if n > 0x7F
            bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
            n >>>= 7
            if n > 0x7F
                bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
                n >>>= 7
                if n > 0x7F
                    bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
                    n >>>= 7
                    if n > 0x7F
                        bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
                        n >>>= 7
                        if n > 0x7F
                            bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
                            n >>>= 7
                            if n > 0x7F
                                bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
                                n >>>= 7
                                if n > 0x7F
                                    bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
                                    n >>>= 7
                                end
                            end
                        end
                    end
                end
            end
        end
    end
    bytes_written += write(stream, n % UInt8)
    bytes_written
end

function encodeFloat(encoder::BinaryEncoder, value::Float32)
    isnan(value) ? write(encoder.stream, NaN32) : write(encoder.stream, value)
end

function encodeDouble(encoder::BinaryEncoder, value::Float64) 
    isnan(value) ? write(encoder.stream, NaN64) : write(encoder.stream, value)
end

encodeByte(encoder::BinaryEncoder, value::UInt8) = write(encoder.stream, value)
encodeBytes(encoder::BinaryEncoder, value::Vector{UInt8}) = write(encoder.stream, value)

function encodeString(encoder::BinaryEncoder, value::String)
    encodeLong(encoder, sizeof(value)) + write(encoder.stream, value)
end

# Decoders

decodeNull(decoder::Decoder) = nothing
decodeBoolean(decoder::Decoder) = read(decoder.stream, Bool)

function decodeInt(decoder::Decoder)
    stream = decoder.stream
    b = read(stream, UInt8) % Int
    n = b & 0x7F
    bytes_read = 1
    while b > 0x7F && bytes_read < 5
        b = read(stream, UInt8) % Int
        n $= (b & 0x7F) << (7 * bytes_read)
        bytes_read += 1
    end
    if b > 0x7F
        throw("Invalid int encoding")
    end

    # Return the results in two's-complement
    (n >>> 1) $ -(n & 1)
end

function decodeLong(decoder::Decoder)
    stream = decoder.stream
    b = read(stream, UInt8) % Int
    n = b & 0x7F
    bytes_read = 1
    while b > 0x7F && bytes_read < 10
        b = read(stream, UInt8) % Int
        n $= (b & 0x7F) << (7 * bytes_read)
        bytes_read += 1
    end
    if b > 0x7F
        throw("Invalid int encoding")
    end

    # Return the results in two's-complement
    (n >>> 1) $ -(n & 1)
end

decodeFloat(decoder::Decoder) = read(decoder.stream, Float32)
decodeDouble(decoder::Decoder) = read(decoder.stream, Float64)
decodeByte(decoder::Decoder) = read(decoder.stream, UInt8)
decodeBytes(decoder::Decoder, nb::Int) = read(decoder.stream, nb)

function decodeString(decoder::Decoder)
    nb = decodeLong(decoder)
    String(decodeBytes(decoder, nb))
end

end
