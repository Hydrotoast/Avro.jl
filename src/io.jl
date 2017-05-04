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

encode_null(::BinaryEncoder, ::Void) = 0
encode_boolean(encoder::BinaryEncoder, value::Bool) = write(encoder.stream, value)

function encode_int(encoder::BinaryEncoder, value::Int32)
    stream = encoder.stream
    n = (value << 1) $ (value >> 31)
    _encode_varint(stream, n, 5)
end

function encode_long(encoder::BinaryEncoder, value::Int64)
    stream = encoder.stream
    n = (value << 1) $ (value >> 63)
    _encode_varint(stream, n, 10)
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

function _encode_varint(stream::IO, n::Integer, max_bytes::Int)
    bytes_written = 0
    while n > 0x7f && bytes_written < max_bytes
        bytes_written += write(stream, ((n | 0x80) & 0xff) % UInt8)
        n >>>= 7
    end
    if n > 0x7f
        throw(Exception("Invalid variable-length integer encoding"))
    end
    bytes_written += write(stream, n % UInt8)
end

# Decoders

decode_null(::Decoder) = nothing
decode_boolean(decoder::Decoder) = read(decoder.stream, Bool)

function decode_int(decoder::Decoder)
    stream = decoder.stream
    n = _decode_varint(stream, 5)

    # Return the results in two's-complement
    (n >>> 1) $ -(n & 1)
end

function decode_long(decoder::Decoder)
    stream = decoder.stream
    n = _decode_varint(stream, 10)

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

function _decode_varint(stream::IO, max_bytes::Integer)
    b = read(stream, UInt8) % Int64
    n = b & 0x7f
    bytes_read = 1
    while b > 0x7f && bytes_read < max_bytes
        b = read(stream, UInt8) % Int64
        n $= (b & 0x7f) << (7 * bytes_read)
        bytes_read += 1
    end
    if b > 0x7f
        throw(Exception("Invalid variable-length integer encoding"))
    end
    n
end

end
