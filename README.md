# Using Avro

[![Build Status](https://travis-ci.org/Hydrotoast/Avro.jl.svg?branch=master)](https://travis-ci.org/Hydrotoast/Avro.jl)

The Julia implementation of the Apache Avro 1.8.1 specification.

Currently, we support the following features:

- Parsing schemas from JSON specifications in `.avsc`
- Reading from and writing to Avro container files
- Generic implementation for instances of Avro values

## Writing to an Avro File

```julia
using Avro

schema_filename = "user.avsc"
output_filename = "users.avro"
output = open(output_filename, "w")

# Parse the schema
schema = open(schema_filename, "r") do file
  Avro.parse(readstring(file))
end

users = [
  GenericRecord(schema, ["bob", 1])
  GenericRecord(schema, ["alice", 2])
]

# Write objects
file_writer = FileWriter.create(schema, output)
for user in users
  FileWriter.append!(file_writer, user)
end
FileWriter.close(file_writer)
```

## Reading from an Avro File

```julia
using Avro

input_filename = "users.avro"
input = open(input_filename, "r")

# Read the objects
for record in FileReader.open(input)
  println(record)
end
```
