module Avro

include("Schema.jl")

export Schemas,
       serialize

import Base.serialize

const BitInt_types      = (Int8, Int16, Int32)
const BitUInt_types     = (UInt8, UInt16, UInt32)
const BitInteger_types  = (BitInt_types..., BitUInt_types...)
const BitLong_types     = (Int64, UInt64)
const BitFloat_types    = (Float16, Float32)
const ByteArray         = Array{UInt8, 1}

# Avro primitive type mappings
typealias AvroNull    Void
typealias AvroBoolean Bool
typealias AvroInt     Union{BitInteger_types...}
typealias AvroLong    Union{BitLong_types...}
typealias AvroFloat   Union{BitFloat_types...}
typealias AvroDouble  Float64
typealias AvroBytes   Array{UInt8, 1}
typealias AvroString  String

function encode_int{T <: AvroInt}(stream::IOBuffer, value::T)
  n = (value << 1) $ (value >> 31)
  info("n: $n")
  if (n & ~0x7F) != 0
    write(stream, ((n | 0x80) & 0xFF) % UInt8)
    n >>= 7
    if (n > 0x7F)
      write(stream, ((n | 0x80) & 0xFF) % UInt8)
      n >>= 7
      if (n > 0x7F)
        write(stream, ((n | 0x80) & 0xFF) % UInt8)
        n >>= 7
        if (n > 0x7F)
          write(stream, ((n | 0x80) & 0xFF) % UInt8)
          n >>= 7
        end
      end
    end
  end
  write(stream,  n)
end

function encode_long{T <: AvroLong}(stream::IOBuffer, value::T)
  n = (value << 1) $ (value >> 64)
  info("n: $n")
  if (n & ~0x7F) != 0
    write(stream, ((n | 0x80) & 0xFF) % UInt8)
    n >>= 7
    if (n > 0x7F)
      write(stream, ((n | 0x80) & 0xFF) % UInt8)
      n >>= 7
      if (n > 0x7F)
        write(stream, ((n | 0x80) & 0xFF) % UInt8)
        n >>= 7
        if (n > 0x7F)
          write(stream, ((n | 0x80) & 0xFF) % UInt8)
          n >>= 7
          if (n > 0x7F)
            write(stream, ((n | 0x80) & 0xFF) % UInt8)
            n >>= 7
            if (n > 0x7F)
              write(stream, ((n | 0x80) & 0xFF) % UInt8)
              n >>= 7
              if (n > 0x7F)
                write(stream, ((n | 0x80) & 0xFF) % UInt8)
                n >>= 7
                if (n > 0x7F)
                  write(stream, ((n | 0x80) & 0xFF) % UInt8)
                  n >>= 7
                end
              end
            end
          end
        end
      end
    end
  end
  write(stream,  n)
end

write(stream::IOBuffer, value::AvroNull) = nothing
write(stream::IOBuffer, value::AvroBoolean) = write
write(stream::IOBuffer, value::AvroInt) = encode_int
write(stream::IOBuffer, value::AvroLong) = encode_long
write(stream::IOBuffer, value::AvroFloat) = write
write(stream::IOBuffer, value::AvroDouble) = write
write(stream::IOBuffer, value::AvroBytes) = write(stream, value)
function write(stream::IOBuffer, value::String)
  write(stream, sizeof(value))
  write(stream, utf8(value))
end

function write(stream::IOBuffer, value::Any)
  # Serialize each of the fields as a record type
  for fieldname in fieldnames(value)
    field_value = getfield(value, fieldname)
    write(stream, field_value)
  end
end


end
