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

buffer = IOBuffer()
encoder = BinaryEncoder(buffer)

@testset "Generic writers" begin
    @testset "Record" for (input, expected) in RECORD_EXAMPLES
        bytes_written = write(encoder, input.schema, input)
        contents = takebuf_array(buffer)

        @test expected == contents
        @test length(expected) == bytes_written
    end
end

end
