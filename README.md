# Avro.jl

[![Build Status](https://travis-ci.org/Hydrotoast/Avro.jl.svg?branch=master)](https://travis-ci.org/Hydrotoast/Avro.jl)

The Julia implementation of the Apache Avro 1.8.1 specification.

Currently, we support the following features:

- Parsing schemas from JSON specifications in `.avsc`
- Reading from and writing to Avro container files
- Generic implementation for instances of Avro values

## User Schema

Consider a simple schema for users.

```json
{
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number",  "type": ["null", "int"]},
        {"name": "favorite_color", "type": ["null", "string"]}
    ]
}
```

We use this schema in the examples below.

## Writing to an Avro File

```julia
using Avro
using Avro.Generic

schema_filename = "user.avsc"
output_filename = "users.avro"
output = open(output_filename, "w")

# Parse the schema
schema = open(schema_filename, "r") do file
    Avro.Schemas.parse(readstring(file))
end

users = [
    GenericRecord(schema, ["bob", Int32(1), "blue"])
    GenericRecord(schema, ["alice", Int32(2), "red"])
]

# Write objects
file_writer = DataFile.Writers.wrap(schema, output)
for user in users
    DataFile.write(file_writer, user)
end
DataFile.close(file_writer)
```

## Reading from an Avro File

```julia
using Avro

input_filename = "users.avro"
input = open(input_filename, "r")

# Read the objects
for record in DataFile.Readers.wrap(input)
    println(record)
end
```

## TODO

- Schema validation
- Schema default values
- Code generation from schemas
