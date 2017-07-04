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

write(encoder::Encoder, ::NullSchema, value::Void) = encode_null(encoder, value)
write(encoder::Encoder, ::BooleanSchema, value::Bool) = encode_boolean(encoder, value)
write(encoder::Encoder, ::IntSchema, value::Int32) = encode_int(encoder, value)
write(encoder::Encoder, ::LongSchema, value::Int64) = encode_long(encoder, value)
write(encoder::Encoder, ::FloatSchema, value::Float32) = encode_float(encoder, value)
write(encoder::Encoder, ::DoubleSchema, value::Float64) = encode_double(encoder, value)
write(encoder::Encoder, ::BytesSchema, value::UInt8) = encode_byte(encoder, value)
write(encoder::Encoder, ::BytesSchema, value::Vector{UInt8}) = encode_bytes(encoder, value)
write(encoder::Encoder, ::StringSchema, value::String) = encode_string(encoder, value)

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

"""
Writes a value as a Union if the schema of the value is in the union.
"""
function write(encoder::Encoder, schema::Schemas.UnionSchema, value)
    datum_schema = resolve_schema(value)
    index = findfirst(schema.schemas, resolve_schema(value)) % Int64
    if index == 0
        throw(Exception("Schema not found in union: $datum_schema"))
    end
    union_tag = index - one(index)
    encode_long(encoder, union_tag) + write(encoder, schema.schemas[index], value)
end

# Generic readers

read(decoder::Decoder, ::NullSchema) = decode_null(decoder)
read(decoder::Decoder, ::BooleanSchema) = decode_boolean(decoder)
read(decoder::Decoder, ::IntSchema) = decode_int(decoder)
read(decoder::Decoder, ::LongSchema) = decode_long(decoder)
read(decoder::Decoder, ::FloatSchema) = decode_float(decoder)
read(decoder::Decoder, ::DoubleSchema) = decode_double(decoder)
read(decoder::Decoder, ::BytesSchema) = decode_bytes(decoder)
read(decoder::Decoder, ::StringSchema) = decode_string(decoder)

"""
Reads array of Avro objects into generic instances.
"""
function read(decoder::Decoder, schema::ArraySchema)
    result = Any[]
    block_count = decode_long(decoder)
    while block_count != 0
        if block_count < 0
            block_count = -block_count
            #  Consume the block_size which is not used
            decode_long(decoder)
        end
        for i in 1:block_count
            push!(result, read(decoder, schema.items))
        end
        block_count = decode_long(decoder)
    end
    result
end

"""
Read maps of Avro objects into generic instances.
"""
function read(decoder::Decoder, schema::MapSchema)
    result = Dict{String, Any}()
    block_count = decode_long(decoder)
    while block_count != 0
        if block_count < 0
            block_count = -block_count
            #  Consume the block_size which is not used
            decode_long(decoder)
        end
        for i in 1:block_count
            key = decode_string(decoder)
            value = read(decoder, schema.values)
            result[key] = value
        end
        block_count = decode_long(decoder)
    end
    result
end

"""
Read unions of Avro objects into generic instances.
"""
function read(decoder::Decoder, schema::UnionSchema)
    union_tag = decode_long(decoder)
    schema = schema.schemas[union_tag + one(union_tag)]
    read(decoder, schema)
end

# Generic implementations for derived types

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
    record.values[field.position + one(field.position)] = v
end

put(record::GenericRecord, v, i::Int) = record[i + one(i)] = v
setindex!(record::GenericRecord, v, i::Int) = record.values[i] = v

function get(record::GenericRecord, key::Symbol)
    field = findfirst(field -> field.name == key, record.schema.fields)
    record.values[field.position + one(field.position)]
end

get(record::GenericRecord, i::Int) = record[i + one(i)]
getindex(record::GenericRecord, i::Int) = record.values[i]

function write(encoder::Encoder, schema::RecordSchema, datum::GenericRecord)
    bytes_written = 0
    for field in schema.fields
        bytes_written += write(encoder, field.schema, get(datum, field.position))
    end
    bytes_written
end

function read(decoder::Decoder, schema::RecordSchema)
    n = length(schema.fields)
    values = Array{Any}(n)
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
    symbol_index = findfirst(schema.symbols, datum.symbol) - one(Int32)
    encode_int(encoder, symbol_index % Int32)
end

function read(decoder::Decoder, schema::EnumSchema)
    symbol_index = decode_int(decoder)
    GenericEnumSymbol(schema, schema.symbols[symbol_index + one(symbol_index)])
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
    @assert schema.size == length(datum.bytes)
    encode_fixed(encoder, datum.bytes)
end

function read(decoder::Decoder, schema::FixedSchema)
    GenericFixed(schema, decode_fixed(decoder, schema.size))
end

# Utilities for generic implementations

resolve_schema(::Void) = Schemas.NULL
resolve_schema(::Bool) = Schemas.BOOLEAN
resolve_schema(::Int32) = Schemas.INT
resolve_schema(::Int64) = Schemas.LONG
resolve_schema(::Float32) = Schemas.FLOAT
resolve_schema(::Float64) = Schemas.DOUBLE
resolve_schema(::Vector{UInt8}) = Schemas.BYTES
resolve_schema(::String) = Schemas.STRING
resolve_schema(::Vector) = ArraySchema
resolve_schema(::Dict) = MapSchema
resolve_schema(datum::GenericRecord) = datum.schema
resolve_schema(datum::GenericEnumSymbol) = datum.schema
resolve_schema(datum::GenericFixed) = datum.schema
resolve_schema(::Any) = throw(Exception("Unknown datum type"))

end
