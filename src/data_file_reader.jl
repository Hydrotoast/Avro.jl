module Readers

import Base.close
import Base: start, next, done

using Avro.DataFile
using Avro.DataFile.Codecs
using Avro.Generic
using Avro.Io
using Avro.Schemas

export wrap,
       close,
       start,
       next,
       done

immutable Reader
    input_decoder::BinaryDecoder
    schema::Schemas.Schema
    codec::Codec
    sync_marker::Vector{UInt8}
end

function wrap(input::IO)
    input_decoder = BinaryDecoder(input)

    header = _read_header(input_decoder)
    magic = getindex(header, 1)
    meta = getindex(header, 2)
    sync_marker = getindex(header, 3).bytes

    # The magic header is invalid
    if magic.bytes != OBJECT_CONTAINER_FILE_MAGIC
        throw("Not an Avro data file.")
    end

    # Parse the schema from the metadata
    schema = Schemas.parse(meta[META_SCHEMA_KEY])
    codec = Codecs.create(get(meta, META_CODEC_KEY, "null"))

    Reader(input_decoder, schema, codec, sync_marker)
end

function close(file_reader::Reader)
    close(file_reader.input_decoder.stream)
end

function start(file_reader::Reader)
    _read_block_header(file_reader)
end

function next(file_reader::Reader, state)
    buffer_decoder, block_count = state
    if block_count == 0
        buffer_decoder, block_count = _read_block_header(file_reader)
    end
    item = read(buffer_decoder, file_reader.schema)
    item, (buffer_decoder, block_count - 1)
end

function done(file_reader::Reader, state)
    buffer_decoder, block_count = state
    if block_count == 0
        sync = read(file_reader.input_decoder, SYNC_SCHEMA)
        @assert file_reader.sync_marker == sync.bytes
    end
    eof(file_reader.input_decoder.stream)
end

"""
Read the header directly the input decoder.
"""
function _read_header(input_decoder::BinaryDecoder)
    read(input_decoder, METADATA_SCHEMA)
end

function _read_block_header(file_reader::Reader)
    input_decoder = file_reader.input_decoder
    block_count = decode_long(input_decoder)
    num_bytes = decode_long(input_decoder)
    block_data = decode_fixed(input_decoder, num_bytes)
    block_data = Codecs.decompress(file_reader.codec, block_data)


    buffer_decoder = BinaryDecoder(IOBuffer(block_data))
    buffer_decoder, block_count
end

end
