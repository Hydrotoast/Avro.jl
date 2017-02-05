module IO

import Base.write

using Avro.Schemas

export Encoder,
       Decoder,
       BinaryEncoder,
       BinaryDecoder,
       DatumWriter,
       DatumReader,
       encode,
       write

abstract Encoder
abstract Decoder

"""
Writes binary data of builtin and primtive types to an output stream.
"""
immutable BinaryEncoder <: Encoder
    stream::IOBuffer
end

"""
Reads data of simple and primitive types from an input stream.
"""
immutable BinaryDecoder <: Decoder
    stream::IOBuffer
end

"""
Writes binary data of user-defined types to an output stream. The user-defined
type is expected to adhere to a schema.
"""
immutable DatumWriter
    encoder::Encoder
    schema::Schema
end

"""
Reads binary data of user-defined types from an input stream. The user-defined
type is expected to adhere to a schema.
"""
immutable DatumReader
    decoder::Decoder
    schema::Schema
end

function encode(encoder::Encoder, value::Int32)
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

function encode(encoder::Encoder, value::Int64)
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

encode(encoder::Encoder, value::Bool) = write(encoder.stream, value)
encode(encoder::Encoder, value::Float32) = write(encoder.stream, value)
encode(encoder::Encoder, value::Float64) = write(encoder.stream, value)
encode(encoder::Encoder, value::UInt8) = write(encoder.stream, value)

function encode(encoder::Encoder, value::String)
    encode(encoder, sizeof(value)) + write(encoder.stream, value)
end

write(encoder::Encoder, schema::IntSchema, value::Int32) = encode(encoder, value)
write(encoder::Encoder, schema::LongSchema, value::Int64) = encode(encoder, value)
write(encoder::Encoder, schema::FloatSchema, value::Float32) = encode(encoder, value)
write(encoder::Encoder, schema::DoubleSchema, value::Float64) = encode(encoder, value)
write(encoder::Encoder, schema::BooleanSchema, value::Bool) = encode(encoder, value)
write(encoder::Encoder, schema::NullSchema, value::Void) = encode(encoder, value)

function write(encoder::Encoder, schema::RecordSchema, value)
    for field in schema.fields
        write(encoder, field.schema, getfield(value, field.name))
    end
end

function write(encoder::Encoder, schema::EnumSchema, value::String)
    index = findfirst(schema.symbols, value) - 1
    encode(encoder, index % Int32)
end

function write{T}(encoder::Encoder, schema::ArraySchema, value::Vector{T})
    encode(encoder, Int64(length(value)))
    for item in value
        write(encoder, schema.items, item)
    end
    encode(encoder, zero(Int32))
end

function write{T}(encoder::Encoder, schema::MapSchema, value::Dict{String, T})
    encode(encoder, Int64(length(value)))
    for (k, v) in value
        encode(encoder, Schemas.string, k)
        write(encoder, schema.values, v)
    end
    encode(encoder, zero(Int32))
end

function write(encoder::Encoder, schema::FixedSchema, value)
    typeof(value).isbits ? encode(encoder, value) : 0
end

write(writer::DatumWriter, value) = write(writer.encoder, writer.schema, value)

end
