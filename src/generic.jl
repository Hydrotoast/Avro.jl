module Generic

import Base: getindex, setindex!
import Base: read, write
import Base.==

using Avro.Schemas
using Avro.Io

export GenericRecord,
       GenericEnumSymbol,
       GenericFixed,
       read,
       write

# Generic writers

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
Writes an array of Avro objects if there is a 
write(Encoder, typeof(ArraySchema.items), T) method.
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
Writes a map of Avro objects if there is a 
write(Encoder, typeof(MapSchema.values), T) method.
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

"""
Contains data for Avro records.
"""
immutable GenericRecord
    schema::RecordSchema
    values::Vector{Any}
end

function ==(a::GenericRecord, b::GenericRecord) 
    a.schema == b.schema && a.values == b.values
end

function put(record::GenericRecord, key::Symbol, v)
    field = findfirst(field -> field.name == key, record.schema.fields)
    record.values[field.position + 1] = v
end

put(record::GenericRecord, v, i::Int) = record[i + 1] = v
setindex!(record::GenericRecord, v, i::Int) = record.values[i] = v

function get(record::GenericRecord, key::Symbol)
    field = findfirst(field -> field.name == key, record.schema.fields)
    record.values[field.position + 1]
end

get(record::GenericRecord, i::Int) = record[i + 1]
getindex(record::GenericRecord, i::Int) = record.values[i]

function write(encoder::Encoder, schema::RecordSchema, datum::GenericRecord)
    bytes_written = 0
    for field in schema.fields
        bytes_written += write(encoder, field.schema, datum[field.position + 1])
    end
    bytes_written
end

function read(decoder::Decoder, schema::RecordSchema)
    n = length(schema.fields)
    values = Array(Any, n)
    for i in 1:n
        values[i] = read(decoder, schema.fields[i].schema)
    end
    GenericRecord(schema, values)
end

"""
An enum symbol.
"""
immutable GenericEnumSymbol
    schema::EnumSchema
    symbol::String
end

function ==(a::GenericEnumSymbol, b::GenericEnumSymbol) 
    a.schema == b.schema && a.symbol == b.symbol
end

function write(encoder::Encoder, schema::EnumSchema, datum::GenericEnumSymbol)
    index = findfirst(schema.symbols, datum.symbol) - 1
    encodeInt(encoder, index % Int32)
end

function read(decoder::Decoder, schema::EnumSchema)
    GenericEnumSymbol(schema, schema.symbols[decodeInt(decoder) + 1])
end

"""
Contains data for Avro fixed objects.
"""
immutable GenericFixed
    schema::FixedSchema
    bytes::Vector{UInt8}
end

function ==(a::GenericFixed, b::GenericFixed) 
    a.schema == b.schema && a.bytes == b.bytes
end

function write(encoder::Encoder, schema::FixedSchema, datum::GenericFixed)
    encodeBytes(encoder, datum.bytes)
end

function read(decoder::Decoder, schema::FixedSchema)
    GenericFixed(schema, decodeBytes(decoder, schema.size))
end

# Generic readers

read(decoder::Decoder, schema::NullSchema) = decodeNull(decoder)
read(decoder::Decoder, schema::BooleanSchema) = decodeBoolean(decoder)
read(decoder::Decoder, schema::IntSchema) = decodeInt(decoder)
read(decoder::Decoder, schema::LongSchema) = decodeLong(decoder)
read(decoder::Decoder, schema::FloatSchema) = decodeFloat(decoder)
read(decoder::Decoder, schema::DoubleSchema) = decodeDouble(decoder)
read(decoder::Decoder, schema::BytesSchema) = decodeByte(decoder)
read(decoder::Decoder, schema::BytesSchema) = decodeBytes(decoder)
read(decoder::Decoder, schema::StringSchema) = decodeString(decoder)

"""
Reads array of Avro objects into generic instances.
"""
function read(decoder::Decoder, schema::ArraySchema)
    n = decodeLong(decoder)
    result = Array(Any, n)
    for i in 1:n
        result[i] = read(decoder, schema.items)
    end
    decodeByte(decoder)
    result
end

"""
Read maps of Avro objects into generic instances.
"""
function read(decoder::Decoder, schema::MapSchema)
    n = decodeLong(decoder)
    result = Dict{String, Any}()
    for i in 1:n
        key = decodeString(decoder)
        value = read(decoder, schema.values)
        result[key] = value
    end
    decodeByte(decoder)
    result
end

end
