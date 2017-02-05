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

const STRING_EXAMPLES = 
    [
        ("foo", Schemas.STRING, [0x06, 0x66, 0x6f, 0x6f])
        ("", Schemas.STRING, [0x00])
    ]

const ARRAY_EXAMPLES = 
    [
        (
            Int64[3, 27], 
            Schemas.ArraySchema(Schemas.LONG), 
            [0x04, 0x06, 0x36, 0x00]
        )
    ]

const MAP_EXAMPLES = 
    [
        (
            Dict{String, Int64}("bar" => 27, "foo" => 3),
            Schemas.MapSchema(Schemas.LONG),
            [0x04, 0x06, 0x62, 0x61, 0x72, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x00]
        )
    ]

buffer = IOBuffer()
encoder = BinaryEncoder(buffer)

@testset "Encoding" begin
    @testset "Integer" for (input, schema, expected) in INT_EXAMPLES
        bytes_written = encode(encoder, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written

        bytes_written = write(encoder, schema, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end

    @testset "Long" for (input, schema, expected) in LONG_EXAMPLES
        bytes_written = encode(encoder, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written

        bytes_written = write(encoder, schema, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end

    @testset "Boolean" for (input, schema, expected) in BOOLEAN_EXAMPLES
        bytes_written = encode(encoder, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written

        bytes_written = write(encoder, schema, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end

    @testset "String" for (input, schema, expected) in STRING_EXAMPLES
        bytes_written = encode(encoder, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written

        bytes_written = write(encoder, schema, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end

    @testset "Array" for (input, schema, expected) in ARRAY_EXAMPLES
        bytes_written = write(encoder, schema, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end

    @testset "Map" for (input, schema, expected) in MAP_EXAMPLES
        bytes_written = write(encoder, schema, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end
end

end
