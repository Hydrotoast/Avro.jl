module Avro 
export serialize

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
typealias AvroString  UTF8String

"""
Writes the string the stream as a UTF8 string
"""
function write_string(stream::IOBuffer, value::AbstractString)
  write(stream, sizeof(value))
  write(stream, utf8(value))
end

function write_fixed(stream::IOBuffer, value::ByteArray)
  write(stream, value)
end

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

function serialize(stream::IOBuffer, value::Any)
  # If the type is a known primitive type
  if isa(value, AvroNull)
    # do nothing
  elseif isa(value, AvroBoolean)
    write(stream, value)
  elseif isa(value, AvroInt)
    encode_int(stream, value)
  elseif isa(value, AvroLong)
    encode_long(stream, value)
  elseif isa(value, AvroFloat)
    write(stream, value)
  elseif isa(value, AvroDouble)
    write(stream, value)
  elseif isa(value, AvroBytes)
    write_fixed(stream, value)
  elseif isa(value, AbstractString)
    write_string(stream, value)
    return
  end

  # Serialize each of the fields as a record type
  for fieldname in fieldnames(value)
    field = getfield(value, fieldname)
    serialize(stream, field)
  end
end

end
