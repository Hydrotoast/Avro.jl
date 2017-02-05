module Schemas

import Base.==
import Base.fullname
import Base.hash
import Base.string

using JSON

export Schema,
       NullSchema,
       BooleanSchema,
       StringSchema,
       BytesSchema,
       IntSchema,
       LongSchema,
       FloatSchema,
       DoubleSchema,
       RecordSchema,
       EnumSchema,
       FixedSchema,
       ArraySchema,
       MapSchema,
       PRIMITIVE_TYPES,
       parse

"""
The set of builtin types.
"""
const PRIMITIVE_TYPES = [
    "null",
    "boolean",
    "int",
    "long",
    "float",
    "double",
    "bytes",
    "string"
    ]

"""
The set of named types.
"""
const NAMED_TYPES = [
    "record",
    "enum",
    "fixed"
    ]

"""
Valid types may appear in the `name` field of a JSON object.
"""
const VALID_TYPES = [
    PRIMITIVE_TYPES;
    NAMED_TYPES;
    ["array", "map", "union"]
    ]

capitalize(s::String) = string(uppercase(s[1]), s[2:end])

function get_required(data::Dict, key::String, error_message::String)
    get(data, key) do
        throw(SchemaParseException(string(error_message, ": ", data)))
    end
end

"""
Exception during Schema parsing.
"""
immutable SchemaParseException <: Exception
    message::String
end

"""
The parent of the Avro Schema hierarchy.
"""
abstract Schema

abstract PrimitiveSchema <: Schema

# Generate the primitive type schemas
for primitive_type in PRIMITIVE_TYPES
    classname = Symbol(capitalize(primitive_type), "Schema")
    @eval begin
        immutable $(classname) <: PrimitiveSchema
        end
    end
    @eval const $(Symbol(primitive_type)) = $(classname)()
end

function create_primitive(typename::String)
    if typename == "null"
        return Schema.null
    elseif typename == "boolean"
        return Schema.boolean
    elseif typename == "string"
        return Schema.string
    elseif typename == "bytes"
        return Schema.bytes
    elseif typename == "int"
        return Schema.int
    elseif typename == "long"
        return Schema.long
    elseif typename == "float"
        return Schema.float
    elseif typename == "double"
        return Schema.double
    else
        throw(Exception("Invalid schema typename"))
    end
end

"""
A fully qualified name in Avro.
"""
immutable FullName
    value::String
end

"""
Construct a fullname from a name and space.
"""
function FullName(name::String, space::String)
    if '.' in name
        FullName(name)
    else
        FullName(space == "" ? name : string(space, '.', name))
    end
end

"""
A context object for maintaining state during schema parsing. The the state
includes the current namespace and a dictionary of the parsed schemas.
"""
type ParseContext
    space::String
    schemas::Dict{FullName, Schema}
end

"""
A schema that can be uniquely identified with a fullname. A fullname is
comprised of a name and a namespace.
"""
abstract NamedSchema <: Schema

"""
Constructs named schemas from a JSON object. The named schema object returned
depends on the type provided by the JSON object.

    "fixed" | "error" => FixedSchema
    "enum" => EnumSchema
    "record" => RecordSchema

If a named schema with the specified name has already been parsed, the
previously parsed schema object is returned from the parse context.
"""
function NamedSchema(json_data::Dict, context::ParseContext, typename::String)
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
@enum Order Ascending Descending Ignore

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
immutable Field
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
type RecordSchema <: NamedSchema
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
    RecordSchema(fullname, doc, aliases, Field[])
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
    fields = Array(Field, length(json_fields))
    for (i, json_field) in enumerate(json_fields)
        fields[i] = Field(json_field, context, i)
    end
    schema.fields = fields

    schema
end

"""
An enum schema.
"""
immutable EnumSchema <: NamedSchema
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
immutable ArraySchema <: Schema
    items::Schema
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
immutable MapSchema <: Schema
    values::Schema
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
immutable UnionSchema <: Schema
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
immutable FixedSchema <: NamedSchema
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
        create_primitive(typename)
    elseif typename in NAMED_TYPES
        NamedSchema(json_data, context, typename)
    elseif typename in VALID_TYPES
        if typename == "array"
            ArraySchema(json_data, context)
        elseif typename == "map"
            MapSchema(json_data, context)
        end
    else
        throw(SchemaParseException("Schema not yet supported: $typename"))
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
        create_primitive(schema_data)
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
==(fullname1::FullName, fullname2::FullName) = fullname1.value == fullname2.value
hash(fullname::FullName) = hash(fullname.value)

"""
Equality definitions for schemas.
"""
=={A <: Schema}(::A, b) = false
=={A <: Schema}(b, ::A) = false
=={A <: PrimitiveSchema}(::A, ::A) = true 
==(a::RecordSchema, b::RecordSchema) = a.fullname == b.fullname
==(a::EnumSchema, b::EnumSchema) = a.fullname == b.fullname && a.symbols == y.symbols
==(a::ArraySchema, b::ArraySchema) = a.items == b.items
==(a::MapSchema, b::MapSchema) = a.values == b.values
==(a::UnionSchema, b::UnionSchema) = a.schemas == b.schemas
==(a::FixedSchema, b::FixedSchema) = a.fullname == b.fullname && a.size == b.size

end
