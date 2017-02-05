module FileTest

using Avro
using Avro.File
using Avro.Generic

using Base.Test

const TEST_RECORD_SCHEMA = Schemas.RecordSchema(
    Schemas.FullName("test"),
    [
        Schemas.Field("a", 0, Schemas.LONG),
        Schemas.Field("b", 1, Schemas.STRING)
        ]
    )

records = [
    GenericRecord(TEST_RECORD_SCHEMA, [Int64(27), "foo"])
]

buffer = IOBuffer()
Avro.create_binary(TEST_RECORD_SCHEMA, buffer) do file_writer
    for record in records
        append(file_writer, record)
    end

    contents = takebuf_array(buffer)
    @test contents[1:4] == Avro.File.OBJECT_CONTAINER_FILE_MAGIC
    @test contents[end - 4:end] == [0x36, 0x06, 0x66, 0x6f, 0x6f]
end

end
