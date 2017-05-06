module DataFile

using Avro.Schemas

export VALID_CODECS,
       OBJECT_CONTAINER_FILE_MAGIC,
       META_CODEC_KEY,
       META_SCHEMA_KEY,
       MAGIC_SCHEMA,
       SYNC_SCHEMA,
       METADATA_SCHEMA

const VALID_CODECS = Set(["null", "deflate"])

"""
Magic constant that is written to the top of every object container file. This
is the hexadecimal form of 'Obj1'.
"""
const OBJECT_CONTAINER_FILE_MAGIC = [0x4f, 0x62, 0x6a, 0x01]

const META_CODEC_KEY = "avro.codec"
const META_SCHEMA_KEY = "avro.schema"

const MAGIC_SCHEMA = Schemas.FixedSchema(Schemas.FullName("Magic"), 4)
const META_SCHEMA = Schemas.MapSchema(Schemas.STRING)
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

include("codec.jl")
include("data_file_writer.jl")
include("data_file_reader.jl")

using Avro.DataFile.Writers
using Avro.DataFile.Readers

end
