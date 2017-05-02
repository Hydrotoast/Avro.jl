module Reader

import Base.close
import Base: open, start, next, done

using Avro.DataFile
using Avro.Generic
using Avro.Io
using Avro.Schemas

using Libz

export open,
       close,
       start,
       next,
       done

immutable DataReader
    input_decoder::BinaryDecoder
    schema::Schemas.Schema
    codec::String
    sync_marker::Vector{UInt8}
end

"""
Read the header directly the input decoder.
"""
function read_header(input_decoder::BinaryDecoder)
    read(input_decoder, METADATA_SCHEMA)
end

function open(input::IO)
    input_decoder = BinaryDecoder(input)

    header = read_header(input_decoder)
    magic = getindex(header, 1)
    meta = getindex(header, 2)
    sync_marker = getindex(header, 3).bytes

    # The magic header is invalid
    if magic.bytes != OBJECT_CONTAINER_FILE_MAGIC
        throw("Not an Avro data file.")
    end

    # Parse the schema from the metadata
    schema = Schemas.parse(meta[META_SCHEMA_KEY])
    codec = get(meta, META_CODEC_KEY, "null")

    DataReader(input_decoder, schema, codec, sync_marker)
end

function close(file_reader::DataReader)
    close(file_reader.input_decoder.stream)
end

function start(file_reader::DataReader)
    read_block_header(file_reader)
end

function next(file_reader::DataReader, state)
    buffer_decoder, block_count = state
    if block_count == 0
        buffer_decoder, block_count = read_block_header(file_reader)
    end
    item = read(buffer_decoder, file_reader.schema)
    item, (buffer_decoder, block_count - 1)
end

function done(file_reader::DataReader, state)
    buffer_decoder, block_count = state
    if block_count == 0
        sync = read(file_reader.input_decoder, SYNC_SCHEMA)
        @assert file_reader.sync_marker == sync.bytes
    end
    eof(file_reader.input_decoder.stream)
end

function read_block_header(file_reader::DataReader)
    input_decoder = file_reader.input_decoder
    block_count = decode_long(input_decoder)
    num_bytes = decode_long(input_decoder)
    block_data = decode_bytes(input_decoder, num_bytes)

    if file_reader.codec == "deflate"
        block_data = Libz.deflate(block_data)
    end

    buffer_decoder = BinaryDecoder(IOBuffer(block_data))
    buffer_decoder, block_count
end

end
