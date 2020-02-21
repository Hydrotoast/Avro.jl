# schema types

@enum Ordering Ascending Descending Ignore

const Optional{T} = Union{Missing, T}
const OptionalTuple{TS} = Tuple{map(x->Optional{x}, TS.types)}
const Some{T} = NTuple{N, T} where N

abstract type Schema; end

const PrimitiveTypes = (;:null => Nothing, :boolean => Bool, :int => Int32,
    :long => Int64, :float => Float32, :double => Float64,
    :bytes => Vector{UInt8}, :string => String)

const PrimativeTypeUnion = Union{values(PrimativeTypes)...}

struct PrimitiveSchema{T<:PrimativeTypeUnion} <: Schema; end

valuetype(::Type{PrimativeSchema{T}}) where T = T

for (k, v) in pairs(PrimativeTypes)
    @eval begin
        canonicalform(::Type{PrimativeSchema{$v}}) = "\"$(string(k))\""
    end
end

struct UnionSchema{N, SS<:NTuple{N, Schema}} <: Schema; end

valuetype(::Type{UnionSchema{N, SS}}) where {N, SS} = Union{map(valuetype, SS.types)...}
canonicalform(::Type{UnionSchema{N, SS}}) where {N, SS} = "[$(join(map(canonicalform, SS.types), ","))]"

struct ArraySchema{S <: Schema} <: Schema; end

valuetype(::Type{ArraySchema{S}}) where S = Vector{valuetype(S)}
canonicalform(::Type{ArraySchema{S}}) where S = "{type:\"array\",items:$(canonicalform(S))}"

struct MapSchema{S <: Schema} <: Schema; end

valuetype(::Type{MapSchema{S}}) where S = Dict{Symbol, valuetype(S)}
canonicalform(::Type{MapSchema{S}}) where S = "{type:\"map\",values:$(canonicalform(S))}"

struct FixedSchema{N, NAME} <: Schema
    aliases::Some{String}
end

valuetype(::Type{FixedSchema{N}}) where N = NTuple{N, UInt8}
canonicalform(::Type{FixedSchema{N, NAME}}) where {N, NAME} = "{name:\"$(string(NAME))\",type:\"fixed\",size:$(Int(N))}"

const ValueTypesTuple{TS} = Tuple{map(valuetype, TS.types)...}

struct RecordSchema{N, NS, SS<:NTuple{N, Schema}, NAME} <: Schema
    doc::Optional{String}
    aliases::Some{String}
    fielddocs::NamedTuple{NS, NTuple{N, Optional{String}}}
    fielddefaults::NamedTuple{NS, OptionalTuple{ValueTypesTuple{SS}}}
    fieldaliases::NamedTuple{NS, NTuple{N, Some{String}}}
    fieldordering::NamedTuple{NS, NTuple{N, Optional{Ordering}}}
end

valuetype(::Type{RecordSchema{N, NS, SS}}) where {N, NS, SS} = NamedTuple{NS, ValueTypesTuple{SS}}
canonicalform(name, schema::Type{<:Schema}) = "{name:\"$(string(name))\",type:$(canonicalform(schema))}"
canonicalform(::Type{RecordSchema{N, NS, SS}}) where {N, NS, SS} = "{name:\"$(string(NAME))\",type:\"record\",fields:[$(join(map(canonicalform, NS, SS.types), ","))]}"

struct EnumSchema{NS, NAME} <: Schema
    doc::Optional{String}
    aliases::Some{String}
    default::Optional{Symbol}
end

valuetype(::Type{EnumSchema}) = Symbol
canonicalform(::Type{EnumSchema{NS, NAME}}) where {NS, NAME} = "{name:\"$(string(NAME))\",type:\"enum\",symbols:[\"$(join(map(string, NS), "\",\""))\"]}"


# schema parsing

struct ParseError <: Exception
    message::String
end
ParseError(msg, data) = ParseError("$msg: $data")

struct ParseContext
    namespace::String
    schemas::Dict{String, Type}
end

function fullname(ctx::ParseContext, name::String)
    ('.' in name || len(ctx.namespace) == 0) ? name : "$namespace.$name"
end

function parse(json::Dict, ctx::ParseContext)
end

function parse_named_schema(json::Dict, ctx::ParseContext, typename::String)
    # Push the current namespace onto the stack
    parent_space = context.space

    # Parse the name, namespace
    name = get_required(json_data, "name", "No name in schema")
    space = parent_space
    if haskey(json_data, "namespace")
        space = json_data["namespace"]
        # Set the current namespace if present
        context.space = space
    end
    fullname = FullName(name, space)

    # Parse the optional aliases and doc
    doc = get(json_data, "doc", "")
    alias_names = get(json_data, "aliases", [])
    aliases = [FullName(name, context.space) for name in alias_names]

    # Return early if a schema with the given fullname already exists
    if haskey(context.schemas, fullname)
        return context.schemas[fullname]
    end

    # Parse the correct schema
    if typename in ["record", "error"]
        schema = RecordSchema(json_data, context, fullname, doc, aliases)
    elseif typename == "fixed"
        schema = FixedSchema(json_data, context, fullname, doc, aliases)
    elseif typename == "enum"
        schema = EnumSchema(json_data, context, fullname, doc, aliases)
    end

    # Pop the namespace on the stack
    context.space = parent_space

    schema
end

"""
An enum of possible orders.
"""

"""
Construct an Order object from a name. This constructor is case-insensitive. If
the name is invalid, throws an exception.
"""
function Order(name::String)
    clean_name = uppercase(name)
    if clean_name == "ASCENDING"
        Ascending
    elseif clean_name == "DESCENDING"
        Descending
    elseif clean_name == "IGNORE"
        Ignore
    else
        throw(Exception("Invalid order name: $name"))
    end
end

"""
A field of a record schema.
"""
struct Field
    name::String
    position::Int
    schema::Schema

    # TODO
    # default::T

    doc::String
    order::Order
    aliases::Vector{FullName}
end

function Field(
        name::String,
        position::Int,
        schema::Schema;
        doc::String = "",
        order::Order = Ascending,
        aliases::Vector{FullName} = FullName[])
    Field(name, position, schema, doc, order, aliases)
end

"""
Construct a field of a record given a JSON object.
"""
function Field(field_data::Dict, context::ParseContext, position::Int)
    name = get_required(field_data, "name", "No field name")
    doc = get(field_data, "doc", "")
    schema = parse_schema(field_data["type"], context)

    # TODO
    # default = field_data["default"]

    order = Order(get(field_data, "order", "ascending"))
    alias_names = get(field_data, "aliases", [])
    aliases = [FullName(name, context.space) for name in alias_names]

    Field(name, position, schema, doc, order, aliases)
end

"""
A record schema.
"""
mutable struct RecordSchema <: NamedSchema
    fullname::FullName
    fields::Vector{Field}
    doc::String
    aliases::Vector{FullName}
end

function RecordSchema(
        fullname::FullName,
        fields::Vector{Field};
        doc::String = "",
        aliases::Vector{FullName} = FullName[])
    RecordSchema(fullname, fields, doc, aliases)
end

function RecordSchema(fullname::FullName, doc::String, aliases::Vector{FullName})
    RecordSchema(fullname, Field[], doc, aliases)
end

"""
Construct a record schema from a JSON object.
"""
function RecordSchema(
        json_data::Dict,
        context::ParseContext,
        fullname::FullName,
        doc::String,
        aliases::Vector{FullName})
    schema = RecordSchema(fullname, doc, aliases)
    context.schemas[fullname] = schema

    # Parse the fields
    json_fields = json_data["fields"]
    fields = Array{Field}(undef, length(json_fields))
    for (i, json_field) in enumerate(json_fields)
        fields[i] = Field(json_field, context, i - 1)
    end
    schema.fields = fields

    schema
end

"""
An enum schema.
"""
struct EnumSchema <: NamedSchema
    fullname::FullName
    symbols::Vector{String}
    doc::String # optional
    aliases::Vector{FullName} # optional
end

function EnumSchema(
        fullname::FullName,
        symbols::Vector{String};
        doc::String = "",
        aliases::Vector{FullName} = FullName[])
    EnumSchema(fullname, symbols, doc, aliases)
end

"""
Construct an enum schema from a JSON object.
"""
function EnumSchema(
        json_data::Dict,
        context::ParseContext,
        fullname::FullName,
        doc::String,
        aliases::Vector{FullName})
    symbols = Array{String}(json_data["symbols"])
    schema = EnumSchema(fullname, symbols, doc, aliases)
    context.schemas[fullname] = schema
    schema
end

"""
An array schema.
"""
struct ArraySchema{T <: Schema} <: Schema
    items::T
end

"""
Construct an array schema from a JSON object.
"""
function ArraySchema(json_data::Dict, context::ParseContext)
    items = parse_schema(json_data["items"], context)
    ArraySchema(items)
end

"""
A map schema.
"""
struct MapSchema{T <: Schema} <: Schema
    values::T
end

"""
Construct a map schema from a JSON object.
"""
function MapSchema(json_data::Dict, context::ParseContext)
    values = parse_schema(json_data["values"], context)
    MapSchema(values)
end

"""
A union schema.
"""
struct UnionSchema <: Schema
    schemas::Vector{Schema}
end

"""
Construct a union schema from a JSON array.
"""
function UnionSchema(json_data::Array, context::ParseContext)
    schemas = map(schema_data -> parse_schema(schema_data, context), json_data)
    UnionSchema(schemas)
end

"""
A fixed schema.
"""
struct FixedSchema <: NamedSchema
    fullname::FullName
    size::Int
    doc::String
    aliases::Vector{FullName}
end

function FixedSchema(
        fullname::FullName,
        size::Int;
        doc::String = "",
        aliases::Vector{FullName} = FullName[])
    FixedSchema(fullname, size, doc, aliases)
end

"""
Construct a fixed schema from a JSON object.
"""
function FixedSchema(
        json_data::Dict,
        context::ParseContext,
        fullname::FullName,
        doc::String,
        aliases::Vector{FullName})
    size = json_data["size"]
    schema = FixedSchema(fullname, size, doc, aliases)
    context.schemas[fullname] = schema
    schema
end

"""
Parse a schema object from a JSON object.
"""
function parse_schema(json_data::Dict, context::ParseContext)
    # Throws KeyError if not found
    typename = get_required(json_data, "type", "No type")
    if typename in PRIMITIVE_TYPES
        PrimitiveSchema(typename)
    elseif typename in NAMED_TYPES
        NamedSchema(json_data, context, typename)
    elseif typename in VALID_TYPES
        if typename == "array"
            ArraySchema(json_data, context)
        elseif typename == "map"
            MapSchema(json_data, context)
        end
    else
        throw(ParseError("Schema not yet supported", typename))
    end
end

"""
Parse a schema object from a JSON object. This schema must be a Union according
to the Avro specification.
"""
function parse_schema(json_data::Array, context::ParseContext)
    UnionSchema(json_data, context)
end

"""
Parse a schema object from a parsed JSON string.
"""
function parse_schema(schema_data::String, context::ParseContext)
    if schema_data in PRIMITIVE_TYPES
        # Choose the primitive schema based on the type name
        PrimitiveSchema(schema_data)
    else
        fullname = FullName(schema_data, context.space)
        get(context.schemas, fullname) do
            fullname = FullName(schema_data)
            context.schemas[fullname]
        end
    end
end

"""
Parse a schema object from a JSON string.
"""
function parse(json_string::String)
    json_data = JSON.parse(json_string)

    # Three cases:
    # 1. JSON object
    # 2. JSON array (union)
    # 3. JSON string (primitive)
    context = ParseContext("", Dict())
    parse_schema(json_data, context)
end

"""
Equality definitions for fullnames.
"""
Base.==(fullname1::FullName, fullname2::FullName) = fullname1.value == fullname2.value
Base.hash(fullname::FullName) = hash(fullname.value)

"""
Equality definitions for schemas.
"""
Base.==(::Schema, ::Schema) = false
Base.==(::A, ::A) where A <: PrimitiveSchema = true
Base.==(a::RecordSchema, b::RecordSchema) = a.fullname == b.fullname
Base.==(a::EnumSchema, b::EnumSchema) = a.fullname == b.fullname && a.symbols == b.symbols
Base.==(a::ArraySchema, b::ArraySchema) = a.items == b.items
Base.==(a::MapSchema, b::MapSchema) = a.values == b.values
Base.==(a::UnionSchema, b::UnionSchema) = a.schemas == b.schemas
Base.==(a::FixedSchema, b::FixedSchema) = a.fullname == b.fullname && a.size == b.size

"""
Show definitions for schemas to show them as JSON.
"""
Base.show(io::IO, name::FullName) = write(io, "\"$(name.value)\"")

for primitive_type in PRIMITIVE_TYPES
    primitive_json = "\"$primitive_type\""
    classname = Symbol(uppercasefirst(primitive_type), "Schema")
    @eval begin
        Base.show(io::IO, ::$classname) = print(io, $primitive_json)
    end
end

function Base.show(io::IO, field::Field)
    write(io, "{\"name\":\"$(field.name)\",\"type\":")
    show(io, field.schema)
    write(io, "}")
end

function Base.show(io::IO, schema::RecordSchema)
    write(io, "{\"name\":")
    show(io, schema.fullname)
    write(io, ",\"type\":\"record\",\"fields\":[")
    show(io, schema.fields[1])
    for field in schema.fields[2:end]
        write(io, ",")
        show(io, field)
    end
    write(io, "]}")
end

function Base.show(io::IO, schema::EnumSchema)
    write(io, "{\"name\":")
    show(io, schema.fullname)
    write(io, ",\"type\":\"enum\",\"symbols\":[")
    write(io, join(["\"$symbol\"" for symbol in schema.symbols], ","))
    write(io, "]}")
end

function Base.show(io::IO, schema::ArraySchema)
    write(io, "{type\":\"array\",\"items\":")
    show(io, schema.items)
    write(io, "}")
end

function Base.show(io::IO, schema::MapSchema)
    write(io, "{type\":\"map\",\"values\":")
    show(io, schema.values)
    write(io, "}")
end

function Base.show(io::IO, schema::UnionSchema)
    write(io, "[")
    show(io, schema.schemas[1])
    for item in schema.schemas[2:end]
        write(io, ",")
        show(io, item)
    end
    write(io, "]")
end

function Base.show(io::IO, schema::FixedSchema)
    write(io, "{\"name\":")
    show(io, schema.fullname)
    write(io, ",\"type\":\"fixed\",\"size\":$(schema.size)}")
end
