module Schemas

import Base.fullname

using Avro
using JSON

export PRIMITIVE_TYPES,
       parse_schema

"""
The set of builtin types.
"""
const PRIMITIVE_TYPES = [
    "null",
    "boolean",
    "string",
    "bytes",
    "int",
    "long",
    "float",
    "double"
    ]

"""
The set of named types.
"""
const NAMED_TYPES = [
    "record",
    "enum",
    "fixed",
    ]

"""
Valid types may appear in the `name` field of a JSON object.
"""
const VALID_TYPES = [
    PRIMITIVE_TYPES;
    NAMED_TYPES;
    ["array", "map", "union"]]

"""
The parent of the Avro Schema hierarchy.
"""
abstract Schema

"""
A context object for maintaining state during schema parsing. The the state
includes the current namespace and a dictionary of the parsed schemas.
"""
immutable ParseContext
    namespace::String
    schemas::Dict{String, Schema}
end

"""
Construct a fullname from a name and namespace.
"""
function fullname(name::String, namespace::String)
    namespace == "" ? name : "$namespace.$name"
end

"""
Parse a fullname string from a JSON string.
"""
function parse_fullname(name::String, context::ParseContext)
    '.' in name ? name : fullname(name, context.namespace)
end

"""
Parse a fullname string from a JSON object.
"""
function parse_fullname(json_data::Dict, context::ParseContext)
    if !haskey(json_data, "name")
        throw(ParseError("No name field to parse from: $json_data"))
    end

    if '.' in json_data["name"]
        json_data["name"]
    else
        name = json_data["name"]
        namespace = get(json_data, "namespace", context.namespace)
        fullname(name, namespace)
    end
end

"""
Split a fullname string into a name and namespace pair.
"""
function split_fullname(fullname::String)
    parts = rsplit(fullname, '.', limit = 2, keep = false)
    parts[2], parts[1]
end

"""
Parse a fullname string into a name and namespace pair from a JSON object.
"""
function split_parse_fullname(json_data::Dict, context::ParseContext)
    if !haskey(json_data, "name")
        throw(ParseError("No name field to parse from: $json_data"))
    end

    if '.' in json_data["name"]
        split_fullname(json_data["name"])
    else
        name = json_data["name"]
        namespace = get(json_data, "namespace", context.namespace)
        name, namespace
    end
end

"""
A schema for a primitive type.
"""
immutable PrimitiveSchema <: Schema
    typename::String
end

"""
A schema that can be uniquely identified with a fullname. A fullname is
comprised of a name and a namespace.
"""
abstract NamedSchema <: Schema

"""
Constructs named schemas from a JSON object. The named schema object returned
depends on the type provided by the JSON object.

    "fixed" => FixedSchema
    "enum" => EnumSchema
    "record" => RecordSchema

If a named schema with the specified name has already been parsed, the
previously parsed schema object is returned from the parse context.
"""
function NamedSchema(json_data, context::ParseContext)
    typename = json_data["type"]

    fullname = parse_fullname(json_data, context)

    # Return early if the context already has a schema with the name specified
    if haskey(context.schemas, fullname)
        return context.schemas[fullname]
    end

    if typename == "fixed"
        FixedSchema(json_data, context)
    elseif typename == "enum"
        EnumSchema(json_data, context)
    elseif typename in ["record", "error"]
        RecordSchema(json_data, context)
    end
end

"""
Return the fullname of a named schema.
"""
fullname(schema::NamedSchema) = fullname(schema.name, schema.namespace)

"""
An enum of possible orders.
"""
@enum Order Ascending Descending Ignore

"""
Construct an Order object from a name. This constructor is case-insensitive. If
the name is invalid, throws an exception.
"""
function Order(name::String)
    clean_name = lowercase(name)
    if clean_name == "ascending"
        Ascending
    elseif clean_name == "descending"
        Descending
    elseif clean_name == "ignore"
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
    doc::String
    schema::Schema

    # TODO
    # default::T

    order::Order
    aliases::Vector{String}
end

"""
Construct a field of a record given a JSON object.
"""
function Field(field_data::Dict, context::ParseContext)
    name = field_data["name"]
    doc = get(field_data, "doc", "")
    schema = parse_schema(field_data["type"], context)

    # TODO
    # default = field_data["default"]

    order = Order(get(field_data, "order", "ascending"))
    aliases = get(field_data, "aliases", [])

    Field(name, doc, schema, order, aliases)
end

"""
A record schema.
"""
immutable RecordSchema <: NamedSchema
    name::String
    namespace::String
    doc::String
    aliases::Vector{String}
    fields::Vector{Field}
end

"""
Construct a record schema from a JSON object.
"""
function RecordSchema(json_data::Dict, context::ParseContext)
    name, namespace = split_parse_fullname(json_data, context)
    new_context = ParseContext(namespace, context.schemas)

    doc = get(json_data, "doc", "")
    aliases = get(json_data, "aliases", [])

    json_fields = json_data["fields"]
    fields = Array(Field, length(json_fields))
    for (i, json_field) in enumerate(json_fields)
        fields[i] = Field(json_field, new_context)
    end

    schema = RecordSchema(name, namespace, doc, aliases, fields)
    context.schemas[fullname(schema)] = schema
    schema
end

"""
An enum schema.
"""
immutable EnumSchema <: NamedSchema
    name::String
    namespace::String
    aliases::Vector{String} # optional
    doc::String # optional
    symbols::Vector{String}
end

"""
Construct an enum schema from a JSON object.
"""
function EnumSchema(json_data, context::ParseContext)
    name, namespace = split_parse_fullname(json_data, context)
    aliases = get(json_data, "aliases", [])
    doc = get(json_data, "doc", "")
    symbols = json_data["symbols"]
    schema = EnumSchema(name, namespace, aliases, doc, symbols)
    context.schemas[fullname(schema)] = schema
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
    schemas::Vector{String}
end

"""
Construct a union schema from a JSON array.
"""
function UnionSchema(json_data::Array, context::ParseContext)
    schemas = map(name -> parse_fullname(name, context), json_data)
    UnionSchema(schemas)
end

"""
A fixed schema.
"""
immutable FixedSchema <: NamedSchema
    name::String
    namespace::String
    size::Int
    aliases::Vector{String}
end

"""
Construct a fixed schema from a JSON object.
"""
function FixedSchema(json_data::Dict, context::ParseContext)
    name, namespace = split_parse_fullname(json_data, context)
    size = json_data["size"]
    aliases = get(json_data, "aliases", [])
    schema = FixedSchema(name, namespace, size, aliases)
    context.schemas[fullname(schema)] = schema
    schema
end

"""
Parse a schema object from JSON data.
"""
function parse_schema(json_data::String)
    # Initialize the parse context
    context = ParseContext("", Dict())
    parse_schema(json_data, context)
end

"""
Parse a schema object from a JSON object.
"""
function parse_schema(json_data::Dict, context::ParseContext)
    # Throws KeyError if not found
    typename = json_data["type"]
    if typename in PRIMITIVE_TYPES
        return PrimitiveSchema(typename)
    elseif typename in NAMED_TYPES
        NamedSchema(json_data, context)
    elseif typename in VALID_TYPES
        if typename == "array"
            ArraySchema(json_data, context)
        elseif typename == "map"
            MapSchema(json_data, context)
        end
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
Parse a schema object from a JSON string.
"""
function parse_schema(raw_schema::String, context::ParseContext)
    if haskey(context.schemas, raw_schema)
        return context.schemas[raw_schema]
    elseif raw_schema in PRIMITIVE_TYPES
        return PrimitiveSchema(raw_schema)
    else
        json_data = JSON.parse(raw_schema)

        # Three cases:
        # 1. JSON object
        # 2. JSON array (union)
        # 3. JSON string (primitive)
        parse_schema(json_data, context)
    end
end

end
