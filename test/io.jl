module IOTest

using Avro.IO
using Avro.Schemas
using Base.Test

const INT_EXAMPLES = [
    (Int32(0), [0x00]),
    (Int32(-1), [0x01]),
    (Int32(1), [0x02]),
    (Int32(-2), [0x03]),
    (Int32(2), [0x04]),
    (Int32(-64), [0x7f]),
    (Int32(64), [0x80, 0x01]),
    (Int32(8192), [0x80, 0x80, 0x01]),
    (Int32(-8193), [0x81, 0x80, 0x01]),
]

const LONG_EXAMPLES = [
    (Int64(0), [0x00]),
    (Int64(-1), [0x01]),
    (Int64(1), [0x02]),
    (Int64(-2), [0x03]),
    (Int64(2), [0x04]),
    (Int64(-64), [0x7f]),
    (Int64(64), [0x80, 0x01]),
    (Int64(8192), [0x80, 0x80, 0x01]),
    (Int64(-8193), [0x81, 0x80, 0x01]),
]

const BOOLEAN_EXAMPLES = [
    (true, [0x01]),
    (false, [0x00]),
]

const STRING_EXAMPLES = [
    ("foo", [0x06, 0x66, 0x6f, 0x6f])
    ("", [0x00])
]

type Test
    a::Int64
    b::String
end

const RECORD_EXAMPLES = [
    (
        Test(Int64(27), "foo"), 
        Schemas.RecordSchema(
            Schemas.FullName("test"),
            "",
            [],
            [
                Schemas.Field("a", 0, Schemas.long, "", Schemas.Order("ASCENDING"), []), 
                Schemas.Field("b", 1, Schemas.string, "", Schemas.Order("ASCENDING"), [])
            ]
        ),
        [0x36, 0x06, 0x66, 0x6f, 0x6f]
    )
]


buffer = IOBuffer()
encoder = BinaryEncoder(buffer)

@testset "Encoding" begin
    @testset "Integer" for (input, expected) in INT_EXAMPLES
        bytes_written = encode(encoder, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end

    @testset "Long" for (input, expected) in LONG_EXAMPLES
        bytes_written = encode(encoder, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end

    @testset "Boolean" for (input, expected) in BOOLEAN_EXAMPLES
        bytes_written = encode(encoder, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end

    @testset "String" for (input, expected) in STRING_EXAMPLES
        bytes_written = encode(encoder, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end

    @testset "Record" for (input, schema, expected) in RECORD_EXAMPLES
        bytes_written = write(encoder, schema, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end
end

end
