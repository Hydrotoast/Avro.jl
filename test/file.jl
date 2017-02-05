module FileTest

using Avro
using Avro.File

using Base.Test

schema = Schemas.RecordSchema(
    Schemas.FullName("test"),
    [
        Schemas.Field("a", 0, Schemas.long),
        Schemas.Field("b", 1, Schemas.string)
        ]
    )

immutable Test
    a::Int64
    b::String
end

tests = [
    Test(27, "foo")
]

buffer = IOBuffer()
Avro.create_binary(schema, buffer) do file_writer
    for test in tests
        append(file_writer, test)
    end

    contents = takebuf_array(buffer)
    @test contents == [0x36, 0x06, 0x66, 0x6f, 0x6f]
end

end
