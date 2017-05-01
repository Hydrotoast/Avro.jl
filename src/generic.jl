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

write(encoder::Encoder, schema::NullSchema, value::Void) = encode_null(encoder, value)
write(encoder::Encoder, schema::BooleanSchema, value::Bool) = encode_boolean(encoder, value)
write(encoder::Encoder, schema::IntSchema, value::Int32) = encode_int(encoder, value)
write(encoder::Encoder, schema::LongSchema, value::Int64) = encode_long(encoder, value)
write(encoder::Encoder, schema::FloatSchema, value::Float32) = encode_float(encoder, value)
write(encoder::Encoder, schema::DoubleSchema, value::Float64) = encode_double(encoder, value)
write(encoder::Encoder, schema::BytesSchema, value::UInt8) = encode_byte(encoder, value)
write(encoder::Encoder, schema::BytesSchema, value::Vector{UInt8}) = encode_bytes(encoder, value)
write(encoder::Encoder, schema::StringSchema, value::String) = encode_string(encoder, value)

"""
Writes an array of Avro objects if there is a 
write(Encoder, typeof(ArraySchema.items), T) method.
"""
function write{T}(encoder::Encoder, schema::ArraySchema, value::Vector{T})
    bytes_written = encode_long(encoder, Int64(length(value)))
    for item in value
        bytes_written += write(encoder, schema.items, item)
    end
    bytes_written += encode_byte(encoder, zero(UInt8))
    bytes_written
end

"""
Writes a map of Avro objects if there is a 
write(Encoder, typeof(MapSchema.values), T) method.
"""
function write{T}(encoder::Encoder, schema::MapSchema, value::Dict{String, T})
    bytes_written = encode_long(encoder, Int64(length(value)))
    for (k, v) in value
        bytes_written += encode_string(encoder, k)
        bytes_written += write(encoder, schema.values, v)
    end
    bytes_written += encode_byte(encoder, zero(UInt8))
    bytes_written
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Void)
    index = findfirst(schema.schemas, Schemas.NULL)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.NULL"))
    end
    encode_int(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Bool)
    index = findfirst(schema.schemas, Schemas.BOOLEAN)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.BOOLEAN"))
    end
    encode_int(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Int32)
    index = findfirst(schema.schemas, Schemas.INT)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.INT"))
    end
    encode_int(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Int64)
    index = findfirst(schema.schemas, Schemas.LONG)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.LONG"))
    end
    encode_int(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Float32)
    index = findfirst(schema.schemas, Schemas.FLOAT)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.FLOAT"))
    end
    encode_int(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Float64)
    index = findfirst(schema.schemas, Schemas.DOUBLE)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.DOUBLE"))
    end
    encode_int(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::Vector{UInt8})
    index = findfirst(schema.schemas, Schemas.BYTES)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.BYTES"))
    end
    encode_int(encoder, index - 1) + write(encoder, schema.schemas[index], value)
end

function write(encoder::Encoder, schema::Schemas.UnionSchema, value::String)
    index = findfirst(schema.schemas, Schemas.STRING)
    if index == 0
        throw(Exception("Schema not found in union: Schemas.STRING"))
    end
    encode_int(encoder, index - 1) + write(encoder, schema.schemas[index], value)
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
    encode_int(encoder, index % Int32)
end

function read(decoder::Decoder, schema::EnumSchema)
    GenericEnumSymbol(schema, schema.symbols[decode_int(decoder) + 1])
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
    encode_bytes(encoder, datum.bytes)
end

function read(decoder::Decoder, schema::FixedSchema)
    GenericFixed(schema, decode_bytes(decoder, schema.size))
end

# Generic readers

read(decoder::Decoder, schema::NullSchema) = decode_null(decoder)
read(decoder::Decoder, schema::BooleanSchema) = decode_boolean(decoder)
read(decoder::Decoder, schema::IntSchema) = decode_int(decoder)
read(decoder::Decoder, schema::LongSchema) = decode_long(decoder)
read(decoder::Decoder, schema::FloatSchema) = decode_float(decoder)
read(decoder::Decoder, schema::DoubleSchema) = decode_double(decoder)
read(decoder::Decoder, schema::BytesSchema) = decode_bytes(decoder)
read(decoder::Decoder, schema::StringSchema) = decode_string(decoder)

"""
Reads array of Avro objects into generic instances.
"""
function read(decoder::Decoder, schema::ArraySchema)
    n = decode_long(decoder)
    result = Array(Any, n)
    for i in 1:n
        result[i] = read(decoder, schema.items)
    end
    decode_byte(decoder)
    result
end

"""
Read maps of Avro objects into generic instances.
"""
function read(decoder::Decoder, schema::MapSchema)
    n = decode_long(decoder)
    result = Dict{String, Any}()
    for i in 1:n
        key = decode_string(decoder)
        value = read(decoder, schema.values)
        result[key] = value
    end
    decode_byte(decoder)
    result
end

end
