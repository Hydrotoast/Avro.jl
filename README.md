# Using Avro

## Writing to an Avro Container File

```julia
using Avro
using Avro.File

schema_filename = "user.avsc"
output_filename = "users.avro"

# Parse the schema
schema = open(schema_filename, "r") do file
  Avro.parse(readstring(file))
end

users = [
  GenericRecord(schema, ["bob", 1])
  GenericRecord(schema, ["alice", 2])
]

# Write objects
Avro.create_binary(schema, output_filename) do file_writer
  for user in users
    File.append!(file_writer, user)
  end
end
```

## Reading from an Avro Container File

```julia
using Avro
using Avro.File

input_filename = "users.avro"
input = open(input_filename, "r")

# Read the objects
for record in File.open(input)
  println(record)
end
```
