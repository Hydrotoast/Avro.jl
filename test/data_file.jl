module FileTest

using Avro
using Avro.DataFile
using Avro.Schemas
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
    GenericRecord(TEST_RECORD_SCHEMA, [Int64(26), "foo"]),
    GenericRecord(TEST_RECORD_SCHEMA, [Int64(27), "foo"]),
    GenericRecord(TEST_RECORD_SCHEMA, [Int64(28), "foo"])
]

@testset "File reading and writing" begin
    buffer = IOBuffer()
    file_writer = DataFile.Writers.wrap(TEST_RECORD_SCHEMA, buffer; sync_interval = 2)
    for record in records
        DataFile.Writers.write(file_writer, record)
    end
    DataFile.Writers.write_block(file_writer)

    contents = take!(buffer)
    @test contents[1:4] == Avro.DataFile.OBJECT_CONTAINER_FILE_MAGIC
    # This block should have 2 records with 10 bytes of data
    @test contents[end - 50] == 0x04 # 2 records
    @test contents[end - 49] == 0x14 # 10 bytes
    @test contents[end - 48:end - 44] == [0x34, 0x06, 0x66, 0x6f, 0x6f]
    @test contents[end - 43:end - 39] == [0x36, 0x06, 0x66, 0x6f, 0x6f]
    @test contents[end - 38:end - 23] == file_writer.sync_marker
    @test contents[end - 22] == 0x02 # 1 records
    @test contents[end - 21] == 0x0a # 5 bytes
    @test contents[end - 20:end - 16] == [0x38, 0x06, 0x66, 0x6f, 0x6f]
    @test contents[end - 15:end] == file_writer.sync_marker

    read_buffer = IOBuffer(contents)
    read_records = GenericRecord[]
    for record in DataFile.Readers.wrap(read_buffer)
        push!(read_records, record)
    end
    @test records == read_records
end

end
