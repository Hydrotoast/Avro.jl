module File

using Avro.Generic
using Avro.Io
using Avro.Schemas

export DataFileWriter,
       DataFileReader,
       append

immutable DataFileWriter
    writer::DatumWriter
end

immutable DataFileReader
    writer::DatumReader
end

const VALID_CODECS = Set(["null", "deflate"])

"""
Magic constant that is written to the top of every object container file. This
is the hexadecimal form of 'Obj1'.
"""
const OBJECT_CONTAINER_FILE_MAGIC = [0x4f, 0x62, 0x6a, 0x01]

const META_CODEC_KEY = "avro.codec"
const META_SCHEMA_KEY = "avro.schema"

const MAGIC_SCHEMA = Schemas.FixedSchema(Schemas.FullName("Magic"), 4)
const META_SCHEMA = Schemas.MapSchema(Schemas.BYTES)
const SYNC_SCHEMA = Schemas.FixedSchema(Schemas.FullName("Sync"), 16)
const METADATA_SCHEMA = 
    Schemas.RecordSchema(
        Schemas.FullName("Header", "org.apache.avro.file"),
        [
            Schemas.Field("magic", 0, MAGIC_SCHEMA),
            Schemas.Field("meta", 1, META_SCHEMA),
            Schemas.Field("sync", 2, SYNC_SCHEMA)
        ]
    )

"""
Generate a 16 byte synchronization marker.
"""
generate_sync_marker() = rand(UInt8, 16)

function string2bytes(a::String)
    buffer = IOBuffer()
    write(buffer, a)
    takebuf_array(buffer)
end

"""
Generates a header for an object container file.
"""
function generate_header(schema; codec::String = "null")
    if !(codec in VALID_CODECS)
        throw(Exception("Invalid codec: $codec"))
    end

    GenericRecord(
        METADATA_SCHEMA,
        [
            GenericFixed(MAGIC_SCHEMA, OBJECT_CONTAINER_FILE_MAGIC),
            Dict(
                META_CODEC_KEY => string2bytes(codec),
                META_SCHEMA_KEY => string2bytes(string(schema)),
            ),
            GenericFixed(SYNC_SCHEMA, generate_sync_marker())
        ]
    )
end

function write_header(file_writer::DataFileWriter)
    header = generate_header(file_writer.writer.schema)
    write(file_writer.writer.encoder, METADATA_SCHEMA, header)
    write(file_writer.writer.encoder, SYNC_SCHEMA, get(header, "sync"))
end

append(file_writer::DataFileWriter, datum) = write(file_writer.writer, datum)

end
