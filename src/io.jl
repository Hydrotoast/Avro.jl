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
       encode_fixed,
       encode_string,
       decode_null,
       decode_boolean,
       decode_int,
       decode_long,
       decode_float,
       decode_double,
       decode_byte,
       decode_bytes,
       decode_fixed,
       decode_string

abstract type Encoder end;
abstract type Decoder end;

"""
Writes binary data of builtin and primtive types to an output stream.
"""
struct BinaryEncoder <: Encoder
    stream::IO
end

"""
Reads data of simple and primitive types from an input stream.
"""
struct BinaryDecoder <: Decoder
    stream::IO
end

# Encoders

encode_null(::BinaryEncoder, ::Nothing) = 0
encode_boolean(encoder::BinaryEncoder, value::Bool) = write(encoder.stream, value)

encode_int(encoder::BinaryEncoder, value::Int32) = _encode_varint(encoder.stream, _encode_zigzag(value))
encode_long(encoder::BinaryEncoder, value::Int64) = _encode_varint(encoder.stream, _encode_zigzag(value))

function encode_float(encoder::BinaryEncoder, value::Float32)
    isnan(value) ? write(encoder.stream, NaN32) : write(encoder.stream, value)
end

function encode_double(encoder::BinaryEncoder, value::Float64)
    isnan(value) ? write(encoder.stream, NaN64) : write(encoder.stream, value)
end

encode_byte(encoder::BinaryEncoder, value::UInt8) = write(encoder.stream, value)

function encode_bytes(encoder::BinaryEncoder, value::Vector{UInt8})
    encode_long(encoder, length(value))
    write(encoder.stream, value)
end

encode_fixed(encoder::BinaryEncoder, value::Vector{UInt8}) = write(encoder.stream, value)

function encode_string(encoder::BinaryEncoder, value::String)
    encode_long(encoder, sizeof(value)) + write(encoder.stream, value)
end

function _encode_zigzag(n::T) where T <: Integer
	num_bits = sizeof(T) * 8
	(n << 1) ⊻ (n >> (num_bits - 1))
end

function _encode_varint(stream::IO, n::T) where T <: Integer
    max_bytes = sizeof(T) + ceil(Int, sizeof(T) / 8)
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

decode_int(decoder::Decoder) = _decode_zigzag(_decode_varint(decoder.stream, Int32))
decode_long(decoder::Decoder) = _decode_zigzag(_decode_varint(decoder.stream, Int64))

decode_float(decoder::Decoder) = read(decoder.stream, Float32)
decode_double(decoder::Decoder) = read(decoder.stream, Float64)

decode_byte(decoder::Decoder) = read(decoder.stream, UInt8)

function decode_bytes(decoder::Decoder)
    nb = decode_long(decoder)
    read(decoder.stream, nb)
end

decode_fixed(decoder::Decoder, nb::Integer) = read(decoder.stream, nb)

function decode_string(decoder::Decoder)
    nb = decode_long(decoder)
    String(decode_fixed(decoder, nb))
end

_decode_zigzag(n::Integer) = (n >>> 1) ⊻ -(n & 1)

function _decode_varint(stream::IO, ::Type{T}) where T <: Integer
    max_bytes = sizeof(T) + ceil(Int, sizeof(T) / 8)
    b = read(stream, UInt8) % T
    n = b & 0x7f
    bytes_read = 1
    while b > 0x7f && bytes_read < max_bytes
        b = read(stream, UInt8) % T
        n ⊻= (b & 0x7f) << (7 * bytes_read)
        bytes_read += 1
    end
    if b > 0x7f
        throw(Exception("Invalid variable-length integer encoding"))
    end
    n
end

end
