using Avro
using Base.Test

type User
  name::ASCIIString
  age::UInt32
end

user = User("hello", 22)
expected_bytes = [
 0x05
 0x00
 0x00
 0x00
 0x00
 0x00
 0x00
 0x00
 0x68
 0x65
 0x6c
 0x6c
 0x6f
 0x2c
]
stream = IOBuffer(length(expected_bytes))
Avro.serialize(stream, user)
@test expected_bytes == stream.data
