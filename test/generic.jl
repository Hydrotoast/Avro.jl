module GenericTest

using Base.Test

using Avro.Schemas
using Avro.Io
using Avro.Generic

const TEST_RECORD_SCHEMA = 
    Schemas.RecordSchema(
        Schemas.FullName("test"),
        [
            Schemas.Field("a", 1, Schemas.long),
            Schemas.Field("b", 2, Schemas.string)
        ]
    )

const RECORD_EXAMPLES =
    [
        (
            GenericRecord(TEST_RECORD_SCHEMA, [Int64(27), "foo"]),
            [0x36, 0x06, 0x66, 0x6f, 0x6f]
        )
    ]

const TEST_ENUM_SCHEMA =
    Schemas.EnumSchema(
        Schemas.FullName("Foo"),
        ["A", "B", "C", "D"]
    )

const ENUM_EXAMPLES =
    [
        (GenericEnumSymbol(TEST_ENUM_SCHEMA, "A"), [0x00]),
        (GenericEnumSymbol(TEST_ENUM_SCHEMA, "D"), [0x06])
    ]

const TEST_FIXED_SCHEMA = Schemas.FixedSchema(Schemas.FullName("md5"), 2)

const FIXED_EXAMPLES =
    [
        (GenericFixed(TEST_FIXED_SCHEMA, [0x01, 0x02]), [0x01, 0x02])
        (GenericFixed(TEST_FIXED_SCHEMA, [0xAA, 0xBB]), [0xAA, 0xBB])
    ]

buffer = IOBuffer()
encoder = BinaryEncoder(buffer)

@testset "Generic writers" begin
    @testset "Record" for (input, expected) in RECORD_EXAMPLES
        bytes_written = write(encoder, input.schema, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end

    @testset "Enum" for (input, expected) in ENUM_EXAMPLES
        bytes_written = write(encoder, input.schema, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end

    @testset "Fixed" for (input, expected) in FIXED_EXAMPLES
        bytes_written = write(encoder, input.schema, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end
end

end
