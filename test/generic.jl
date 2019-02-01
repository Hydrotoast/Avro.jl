module GenericTest

using Test

using Avro.Schemas
using Avro.Io
using Avro.Generic

const TEST_RECORD_SCHEMA =
    Schemas.RecordSchema(
        Schemas.FullName("test"),
        [
            Schemas.Field("a", 0, Schemas.LONG),
            Schemas.Field("b", 1, Schemas.STRING)
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
    Schemas.EnumSchema(Schemas.FullName("Foo"), ["A", "B", "C", "D"])

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

const UNION_EXAMPLES =
    [
        (Int64(2), Schemas.UnionSchema([Schemas.LONG, Schemas.NULL]), [0x00, 0x04]),
        (nothing, Schemas.UnionSchema([Schemas.LONG, Schemas.NULL]), [0x02])
    ]


buffer = IOBuffer()
encoder = BinaryEncoder(buffer)
decoder = BinaryDecoder(buffer)

@testset "Generic writers" begin
    @testset "Record" for (input, expected) in RECORD_EXAMPLES
        # Encode the datum
        bytes_written = write(encoder, input.schema, input)

        # Decode the datum
        seekstart(buffer)
        output = read(decoder, input.schema)

        # Inspect the contents of the buffer
        contents = take!(buffer)

        @test expected == contents
        @test input == output
        @test length(expected) == bytes_written
    end

    @testset "Enum" for (input, expected) in ENUM_EXAMPLES
        # Encode the datum
        bytes_written = write(encoder, input.schema, input)

        # Decode the datum
        seekstart(buffer)
        output = read(decoder, input.schema)

        # Inspect the contents of the buffer
        contents = take!(buffer)

        @test expected == contents
        @test input == output
        @test length(expected) == bytes_written
    end

    @testset "Fixed" for (input, expected) in FIXED_EXAMPLES
        # Encode the datum
        bytes_written = write(encoder, input.schema, input)

        # Decode the datum
        seekstart(buffer)
        output = read(decoder, input.schema)

        # Inspect the contents of the buffer
        contents = take!(buffer)

        @test expected == contents
        @test input == output
        @test length(expected) == bytes_written
    end

    @testset "Array" for (input, schema, expected) in ARRAY_EXAMPLES
        # Encode the datum
        bytes_written = write(encoder, schema, input)

        # Decode the datum
        seekstart(buffer)
        output = read(decoder, schema)

        # Inspect the contents of the buffer
        contents = take!(buffer)

        @test expected == contents
        @test input == output
        @test length(expected) == bytes_written
    end

    @testset "Map" for (input, schema, expected) in MAP_EXAMPLES
        # Encode the datum
        bytes_written = write(encoder, schema, input)

        # Decode the datum
        seekstart(buffer)
        output = read(decoder, schema)

        # Inspect the contents of the buffer
        contents = take!(buffer)

        @test expected == contents
        @test input == output
        @test length(expected) == bytes_written
    end

    @testset "Union" for (input, schema, expected) in UNION_EXAMPLES
        # Encode the datum
        bytes_written = write(encoder, schema, input)

        # Decode the datum
        seekstart(buffer)
        output = read(decoder, schema)

        # Inspect the contents of the buffer
        contents = take!(buffer)

        @test expected == contents
        @test input == output
        @test length(expected) == bytes_written
    end
end

end
