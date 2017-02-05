module Generic

using Avro.Schemas
using Avro.Io

import Avro.Io.write

export GenericRecord,
       GenericEnumSymbol,
       GenericFixed

"""
Contains data for Avro records.
"""
immutable GenericRecord
    schema::RecordSchema
    values::Vector{Any}
end

function put(record::GenericRecord, key::Symbol, v)
    field = findfirst(field -> field.name == key, record.schema.fields)
    record.values[field.position] = v
end

function put(record::GenericRecord, i::Int, v)
    record.values[i] = v
end

function get(record::GenericRecord, key::Symbol)
    field = findfirst(field -> field.name == key, record.schema.fields)
    record.values[field.position]
end

function get(record::GenericRecord, i::Int)
    record.values[i]
end

function write(encoder::Encoder, schema::RecordSchema, datum::GenericRecord)
    bytes_written = 0
    for field in schema.fields
        bytes_written += write(encoder, field.schema, get(datum, field.position))
    end
    bytes_written
end

"""
An enum symbol.
"""
immutable GenericEnumSymbol
    schema::EnumSchema
    symbol::String
end

function write(encoder::Encoder, schema::EnumSchema, datum::GenericEnumSymbol)
    index = findfirst(schema.symbols, datum.symbol) - 1
    encode(encoder, index % Int32)
end

"""
Contains data for Avro fixed objects.
"""
immutable GenericFixed{N}
    schema::FixedSchema
    bytes::Vector{UInt8}
end

function write{N}(encoder::Encoder, schema::FixedSchema, datum::GenericFixed{N})
    encode(encoder, datum.bytes)
end

end
