module Io

import Base.write

using Avro.Common
using Avro.Schemas

export Encoder,
       Decoder,
       BinaryEncoder,
       BinaryDecoder,
       encode_null,
       encode_boolean,
       encode_int,
       encode_long,
       encode_float,
       encode_double,
       encode_byte,
       encode_bytes,
       encode_string,
       decode_null,
       decode_boolean,
       decode_int,
       decode_long,
       decode_float,
       decode_double,
       decode_byte,
       decode_bytes,
       decode_string

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

encode_null(encoder::BinaryEncoder, value::Void) = 0
encode_boolean(encoder::BinaryEncoder, value::Bool) = write(encoder.stream, value)

function encode_int(encoder::BinaryEncoder, value::Int32)
    stream = encoder.stream
    bytes_written = 0
    n = (value << 1) $ (value >> 31)
    while n > 0x7F && bytes_written < 4
        bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
        n >>>= 7
    end
    bytes_written += write(stream, n % UInt8)
    bytes_written
end

function encode_long(encoder::BinaryEncoder, value::Int64)
    stream = encoder.stream
    bytes_written = 0
    n = (value << 1) $ (value >> 63)
    while n > 0x7F && bytes_written < 8
        bytes_written += write(stream, ((n | 0x80) & 0xFF) % UInt8)
        n >>>= 7
    end
    bytes_written += write(stream, n % UInt8)
    bytes_written
end

function encode_float(encoder::BinaryEncoder, value::Float32)
    isnan(value) ? write(encoder.stream, NaN32) : write(encoder.stream, value)
end

function encode_double(encoder::BinaryEncoder, value::Float64) 
    isnan(value) ? write(encoder.stream, NaN64) : write(encoder.stream, value)
end

encode_byte(encoder::BinaryEncoder, value::UInt8) = write(encoder.stream, value)
encode_bytes(encoder::BinaryEncoder, value::Vector{UInt8}) = write(encoder.stream, value)

function encode_string(encoder::BinaryEncoder, value::String)
    encode_long(encoder, sizeof(value)) + write(encoder.stream, value)
end

# Decoders

decode_null(decoder::Decoder) = nothing
decode_boolean(decoder::Decoder) = read(decoder.stream, Bool)

function decode_int(decoder::Decoder)
    stream = decoder.stream
    b = read(stream, UInt8) % Int32
    n = b & 0x7F
    bytes_read = 1
    while b > 0x7F && bytes_read < 5
        b = read(stream, UInt8) % Int32
        n $= (b & 0x7F) << (7 * bytes_read)
        bytes_read += 1
    end
    if b > 0x7F
        throw("Invalid int encoding")
    end

    # Return the results in two's-complement
    (n >>> 1) $ -(n & 1)
end

function decode_long(decoder::Decoder)
    stream = decoder.stream
    b = read(stream, UInt8) % Int64
    n = b & 0x7F
    bytes_read = 1
    while b > 0x7F && bytes_read < 10
        b = read(stream, UInt8) % Int64
        n $= (b & 0x7F) << (7 * bytes_read)
        bytes_read += 1
    end
    if b > 0x7F
        throw("Invalid int encoding")
    end

    # Return the results in two's-complement
    (n >>> 1) $ -(n & 1)
end

decode_float(decoder::Decoder) = read(decoder.stream, Float32)
decode_double(decoder::Decoder) = read(decoder.stream, Float64)
decode_byte(decoder::Decoder) = read(decoder.stream, UInt8)
decode_bytes(decoder::Decoder, nb::Int) = read(decoder.stream, nb)

function decode_string(decoder::Decoder)
    nb = decode_long(decoder)
    String(decode_bytes(decoder, nb))
end

end
