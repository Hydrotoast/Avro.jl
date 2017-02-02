# Using Avro

## Serializing

```julia
using Avro
using Avro.IO

# Parse the schema
schema = Schemas.parse_json("user.avsc")

# Write objects
filename = "users.avro"

users = [
  User("bob", 1)
  User("alice", 2)
]

writer = DatumWriter(BinaryEncoder(), schema)

create(writer, filename) do file_writer
  for user in users
    append(file_writer, user)
  end
end
```
