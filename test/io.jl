module IoTest

using Avro.Io
using Avro.Schemas
using Base.Test

const BOOLEAN_EXAMPLES = 
    [
        (true, Schemas.BOOLEAN, [0x01]),
        (false, Schemas.BOOLEAN, [0x00]),
    ]

const INT_EXAMPLES = 
    [
        (Int32(0), Schemas.INT, [0x00]),
        (Int32(-1), Schemas.INT, [0x01]),
        (Int32(1), Schemas.INT, [0x02]),
        (Int32(-2), Schemas.INT, [0x03]),
        (Int32(2), Schemas.INT, [0x04]),
        (Int32(-64), Schemas.INT, [0x7f]),
        (Int32(64), Schemas.INT, [0x80, 0x01]),
        (Int32(8192), Schemas.INT, [0x80, 0x80, 0x01]),
        (Int32(-8193), Schemas.INT, [0x81, 0x80, 0x01]),
    ]

const LONG_EXAMPLES = 
    [
        (Int64(0), Schemas.LONG, [0x00]),
        (Int64(-1), Schemas.LONG, [0x01]),
        (Int64(1), Schemas.LONG, [0x02]),
        (Int64(-2), Schemas.LONG, [0x03]),
        (Int64(2), Schemas.LONG, [0x04]),
        (Int64(-64), Schemas.LONG, [0x7f]),
        (Int64(64), Schemas.LONG, [0x80, 0x01]),
        (Int64(8192), Schemas.LONG, [0x80, 0x80, 0x01]),
        (Int64(-8193), Schemas.LONG, [0x81, 0x80, 0x01]),
    ]

const FLOAT_EXAMPLES =
    [
        (Float32(0), Schemas.FLOAT, zeros(UInt8, 4)),
        (Float32(0.1), Schemas.FLOAT, [0xCD, 0xCC, 0xCC, 0x3D]),
        (Float32(0.2), Schemas.FLOAT, [0xCD, 0xCC, 0x4C, 0x3E]),
        (Float32(1.0), Schemas.FLOAT, [0x00, 0x00, 0x80, 0x3f]),
        (Inf32, Schemas.FLOAT, [0x00, 0x00, 0x80, 0x7F]),
        (-Inf32, Schemas.FLOAT, [0x00, 0x00, 0x80, 0xFF]),
        (NaN32, Schemas.FLOAT, [0x00, 0x00, 0xC0, 0x7F]),
    ]

const DOUBLE_EXAMPLES =
    [
        (Float64(0), Schemas.DOUBLE, zeros(UInt8, 8)),
        (Float64(0.1), Schemas.DOUBLE, [0x9A, 0x99, 0x99, 0x99, 0x99, 0x99, 0xb9, 0x3F]),
        (Float64(0.2), Schemas.DOUBLE, [0x9A, 0x99, 0x99, 0x99, 0x99, 0x99, 0xc9, 0x3F]),
        (Float64(1.0), Schemas.DOUBLE, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F]),
        (Inf64, Schemas.DOUBLE, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x7F]),
        (-Inf64, Schemas.DOUBLE, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xFF]),
        (NaN64, Schemas.DOUBLE, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x7F]),
    ]

const STRING_EXAMPLES = 
    [
        ("foo", Schemas.STRING, [0x06, 0x66, 0x6f, 0x6f])
        ("", Schemas.STRING, [0x00])
    ]

buffer = IOBuffer()
encoder = BinaryEncoder(buffer)
decoder = BinaryDecoder(buffer)

@testset "Encoding" begin
    @testset "Integer" for (input, schema, expected) in INT_EXAMPLES
        # Encode the datum
        bytes_written = encode_int(encoder, input)

        # Decode the datum
        seekstart(buffer)
        output = decode_int(decoder)

        # Inspect the contents of the buffer
        contents = take!(buffer)

        @test expected == contents
        @test input == output
        @test length(expected) == bytes_written

        take!(buffer)
    end

    @testset "Long" for (input, schema, expected) in LONG_EXAMPLES
        # Encode the datum
        bytes_written = encode_long(encoder, input)

        # Decode the datum
        seekstart(buffer)
        output = decode_long(decoder)

        # Inspect the contents of the buffer
        contents = take!(buffer)

        @test expected == contents
        @test input == output
        @test length(expected) == bytes_written
    end

    @testset "Float" for (input, schema, expected) in FLOAT_EXAMPLES
        # Encode the datum
        bytes_written = encode_float(encoder, input)

        # Decode the datum
        seekstart(buffer)
        output = decode_float(decoder)

        # Inspect the contents of the buffer
        contents = take!(buffer)

        @test expected == contents
        if !isnan(input)
            @test input == output
        end
        @test length(expected) == bytes_written
    end

    @testset "Double" for (input, schema, expected) in DOUBLE_EXAMPLES
        # Encode the datum
        bytes_written = encode_double(encoder, input)

        # Decode the datum
        seekstart(buffer)
        output = decode_double(decoder)

        # Inspect the contents of the buffer
        contents = take!(buffer)

        @test expected == contents
        if !isnan(input)
            @test input == output
        end
        @test length(expected) == bytes_written
    end

    @testset "Boolean" for (input, schema, expected) in BOOLEAN_EXAMPLES
        # Encode the datum
        bytes_written = encode_boolean(encoder, input)

        # Decode the datum
        seekstart(buffer)
        output = decode_boolean(decoder)

        # Inspect the contents of the buffer
        contents = take!(buffer)

        @test expected == contents
        @test input == output
        @test length(expected) == bytes_written
    end

    @testset "String" for (input, schema, expected) in STRING_EXAMPLES
        # Encode the datum
        bytes_written = encode_string(encoder, input)

        # Decode the datum
        seekstart(buffer)
        output = decode_string(decoder)

        # Inspect the contents of the buffer
        contents = take!(buffer)

        @test expected == contents
        @test output == input
        @test length(expected) == bytes_written
    end
end

end
