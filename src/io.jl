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

encodeFloat(encoder::BinaryEncoder, value::Float32) = write(encoder.stream, value)
encodeDouble(encoder::BinaryEncoder, value::Float64) = write(encoder.stream, value)
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

# Default writers

write(encoder::Encoder, schema::NullSchema, value::Void) = encodeNull(encoder, value)
write(encoder::Encoder, schema::BooleanSchema, value::Bool) = encodeBoolean(encoder, value)
write(encoder::Encoder, schema::IntSchema, value::Int32) = encodeInt(encoder, value)
write(encoder::Encoder, schema::LongSchema, value::Int64) = encodeLong(encoder, value)
write(encoder::Encoder, schema::FloatSchema, value::Float32) = encodeFloat(encoder, value)
write(encoder::Encoder, schema::DoubleSchema, value::Float64) = encodeDouble(encoder, value)
write(encoder::Encoder, schema::BytesSchema, value::UInt8) = encodeByte(encoder, value)
write(encoder::Encoder, schema::BytesSchema, value::Vector{UInt8}) = encodeBytes(encoder, value)
write(encoder::Encoder, schema::StringSchema, value::String) = encodeString(encoder, value)

"""
Natively write arrays to Avro.
"""
function write{T}(encoder::Encoder, schema::ArraySchema, value::Vector{T})
    bytes_written = encodeLong(encoder, Int64(length(value)))
    for item in value
        bytes_written += write(encoder, schema.items, item)
    end
    bytes_written += encodeByte(encoder, zero(UInt8))
    bytes_written
end

"""
Natively write maps to Avro.
"""
function write{T}(encoder::Encoder, schema::MapSchema, value::Dict{String, T})
    bytes_written = encodeLong(encoder, Int64(length(value)))
    for (k, v) in value
        bytes_written += encodeString(encoder, k)
        bytes_written += write(encoder, schema.values, v)
    end
    bytes_written += encodeByte(encoder, zero(UInt8))
    bytes_written
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Void)
    index = findfirst(schema.schemas, Schemas.NULL)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.NULL"))
    end
    encodeInt(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Bool)
    index = findfirst(schema.schemas, Schemas.BOOLEAN)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.BOOLEAN"))
    end
    encodeInt(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Int32)
    index = findfirst(schema.schemas, Schemas.INT)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.INT"))
    end
    encodeInt(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Int64)
    index = findfirst(schema.schemas, Schemas.LONG)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.LONG"))
    end
    encodeInt(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Float32)
    index = findfirst(schema.schemas, Schemas.FLOAT)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.FLOAT"))
    end
    encodeInt(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Float64)
    index = findfirst(schema.schemas, Schemas.DOUBLE)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.DOUBLE"))
    end
    encodeInt(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Vector{UInt8})
    index = findfirst(schema.schemas, Schemas.BYTES)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.BYTES"))
    end
    encodeInt(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::String)
    index = findfirst(schema.schemas, Schemas.STRING)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.STRING"))
    end
    encodeInt(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

end
