module File

import Base.close

using Avro.Generic
using Avro.Io
using Avro.Schemas

export DataFileWriter,
       DataFileReader

type DataFileWriter
    schema::Schemas.Schema
    output_encoder::BinaryEncoder
    buffer_encoder::BinaryEncoder

    # Users may modify these settings
    codec::String
    sync_marker::Vector{UInt8}
    sync_interval::Int

    # Internal, mutable state
    block_count::Int
end

function DataFileWriter(
        schema::Schemas.Schema,
        output_encoder::BinaryEncoder,
        buffer_encoder::BinaryEncoder,
        codec::String,
        sync_marker::Vector{UInt8},
        sync_interval::Int)
    DataFileWriter(
        schema,
        output_encoder,
        buffer_encoder,
        codec,
        sync_marker,
        sync_interval,
        0)
end

immutable DataFileReader
    schema::Schemas.Schema
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

"""
Generate a 16 byte synchronization marker.
"""
generate_sync_marker() = rand(UInt8, 16)

"""
Generates a header for an object container file.
"""
function generate_header(schema, sync_marker, codec::String)
    if !(codec in VALID_CODECS)
        throw(Exception("Invalid codec: $codec"))
    end

    GenericRecord(
        METADATA_SCHEMA,
        [
            GenericFixed(MAGIC_SCHEMA, OBJECT_CONTAINER_FILE_MAGIC),
            Dict(
                META_CODEC_KEY => codec,
                META_SCHEMA_KEY => string(schema),
            ),
            GenericFixed(SYNC_SCHEMA, sync_marker)
        ]
    )
end

function write_header(file_writer::DataFileWriter)
    header = generate_header(
        file_writer.schema, 
        file_writer.sync_marker,
        file_writer.codec)

    # Write the header directly the output encoder
    write(file_writer.output_encoder, METADATA_SCHEMA, header)
end

function create(
        schema::Schemas.Schema, 
        output::IO;
        codec::String = "null",
        sync_marker::Vector{UInt8} = generate_sync_marker(),
        sync_interval::Int = 256)

    # Initialize the output and buffer encoders
    output_encoder = BinaryEncoder(output)
    buffer_encoder = BinaryEncoder(IOBuffer(Int(sync_interval * 1.25)))

    # Initialize the file writer
    file_writer = DataFileWriter(
        schema, 
        output_encoder, 
        buffer_encoder, 
        codec, 
        sync_marker, 
        sync_interval)

    # Write the header
    write_header(file_writer)
    
    # Return the file writer
    file_writer
end

function append(file_writer::DataFileWriter, datum)
    buffer_encoder = file_writer.buffer_encoder
    schema = file_writer.schema
    
    # Write the datum to the buffer encoder using given schema
    write(buffer_encoder, schema, datum)

    # Increment the number of records in the block
    file_writer.block_count += 1

    # Flush the buffer to the output if we have reached the sync interval
    if file_writer.block_count >= file_writer.sync_interval
        write_block(file_writer)
    end
end

function close(file_writer::DataFileWriter)
    write_block(file_writer)

    close(file_writer.output_encoder.stream)
end

function write_block(file_writer::DataFileWriter)
    if file_writer.block_count > 0
        buffer_encoder = file_writer.buffer_encoder
        output_encoder = file_writer.output_encoder

        # Extract the buffer data as bytes and reset the buffer state
        buffer_data = takebuf_array(buffer_encoder.stream)

        # Write number of records, the blocks size (bytes), the data, and then
        # the sync marker
        encodeLong(output_encoder, file_writer.block_count)
        encodeLong(output_encoder, length(buffer_data))
        encodeBytes(output_encoder, buffer_data)
        encodeBytes(output_encoder, file_writer.sync_marker)

        # Reset the block counter
        file_writer.block_count = 0
    end
end

end
