module Writers

using Avro.DataFile
using Avro.DataFile.Codecs
using Avro.Generic
using Avro.Io
using Avro.Schemas

export wrap

mutable struct Writer{CodecName}
    schema::Schemas.Schema
    output_encoder::BinaryEncoder
    buffer_encoder::BinaryEncoder

    # Users may modify these settings
    codec::Codec{CodecName}
    sync_marker::Vector{UInt8}
    sync_interval::Int

    # Internal, mutable state
    block_count::Int
end

function Writer(
        schema::Schemas.Schema,
        output_encoder::BinaryEncoder,
        buffer_encoder::BinaryEncoder,
        codec::Codec{CodecName},
        sync_marker::Vector{UInt8},
        sync_interval::Int) where CodecName
    Writer(
        schema,
        output_encoder,
        buffer_encoder,
        codec,
        sync_marker,
        sync_interval,
        0)
end

function wrap(
        schema::Schemas.Schema,
        output::IO;
        codec_name::String = "null",
        sync_marker::Vector{UInt8} = _generate_sync_marker(),
        sync_interval::Integer = 256)

    # Initialize the output and buffer encoders
    output_encoder = BinaryEncoder(output)
    buffer_encoder = BinaryEncoder(IOBuffer())
    codec = Codecs.create(codec_name)

    # Initialize the file writer
    file_writer = Writer(
        schema,
        output_encoder,
        buffer_encoder,
        codec,
        sync_marker,
        sync_interval)

    # Write the header
    _write_header(file_writer)

    # Return the file writer
    file_writer
end

function Base.write(file_writer::Writer, datum)
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

function Base.close(file_writer::Writer)
    write_block(file_writer)

    close(file_writer.output_encoder.stream)
end

function write_block(file_writer::Writer)
    if file_writer.block_count > 0
        buffer_encoder = file_writer.buffer_encoder
        output_encoder = file_writer.output_encoder
        codec = file_writer.codec

        # Extract the buffer data as bytes, compress the buffer data, and
        # reset the buffer state
        buffer_data = take!(buffer_encoder.stream)
        buffer_data = Codecs.compress(codec, buffer_data)

        # Write number of records, the blocks size (bytes), the data, and then
        # the sync marker
        encode_long(output_encoder, file_writer.block_count)
        encode_long(output_encoder, length(buffer_data))
        encode_fixed(output_encoder, buffer_data)
        encode_fixed(output_encoder, file_writer.sync_marker)

        # Reset the block counter
        file_writer.block_count = 0
    end
end

"""
Generate a 16 byte synchronization marker.
"""
_generate_sync_marker() = rand(UInt8, 16)

"""
Generates a header for an object container file.
"""
function _generate_header(schema, sync_marker, codec::Codec)
    GenericRecord(
        METADATA_SCHEMA,
        [
            GenericFixed(MAGIC_SCHEMA, OBJECT_CONTAINER_FILE_MAGIC),
            Dict(
                META_CODEC_KEY => Codecs.name(codec),
                META_SCHEMA_KEY => string(schema),
            ),
            GenericFixed(SYNC_SCHEMA, sync_marker)
        ]
    )
end

function _write_header(file_writer::Writer)
    header = _generate_header(
        file_writer.schema,
        file_writer.sync_marker,
        file_writer.codec)

    # Write the header directly the output encoder
    write(file_writer.output_encoder, METADATA_SCHEMA, header)
end

end
