module Readers

using Avro.DataFile
using Avro.DataFile.Codecs
using Avro.Generic
using Avro.Io
using Avro.Schemas

export wrap

struct Reader
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

function Base.close(file_reader::Reader)
    close(file_reader.input_decoder.stream)
end

function start(file_reader::Reader)
    _read_block_header(file_reader)
end

# The invariant maintained by the iterator is that block_count = 0 only at the end of the
# stream. If block_count does drop to 0, we verify the 16 sync bytes and then read block
# headers until one with size > 0 is found.
function next(file_reader::Reader, state)
    buffer_decoder, block_count = state
    @assert block_count != 0
    item = read(buffer_decoder, file_reader.schema)
    block_count -= 1
    if block_count == 0
        sync = read(file_reader.input_decoder, SYNC_SCHEMA)
        @assert file_reader.sync_marker == sync.bytes
        buffer_decoder, block_count = _read_block_header(file_reader)
    end
    item, (buffer_decoder, block_count)
end

function done(state)
    decoder, block_count = state
    block_count == 0
end

function Base.iterate(file_reader::Reader, state)
    done(state) ? nothing : next(file_reader, state)
end
Base.iterate(file_reader::Reader) = iterate(file_reader, start(file_reader))

Base.IteratorSize(reader::Reader) = Base.SizeUnknown()

"""
Read the header directly the input decoder.
"""
function _read_header(input_decoder::BinaryDecoder)
    read(input_decoder, METADATA_SCHEMA)
end

# Always returns block_count > 0, except in the case of eof, when we return (nothing, 0)
# This allows us to keep the invariant that block_count is always > 0 unless at end of stream.
function _read_block_header(file_reader::Reader)
    block_count = 0
    block_data = nothing
    while block_count == 0
        if (eof(file_reader.input_decoder.stream))
            return (nothing, 0)
        end
        input_decoder = file_reader.input_decoder
        block_count = decode_long(input_decoder)
        num_bytes = decode_long(input_decoder)
        block_data = decode_fixed(input_decoder, num_bytes)
        block_data = Codecs.decompress(file_reader.codec, block_data)
        if block_count == 0
            sync = read(input_decoder, SYNC_SCHEMA)
            @assert file_reader.sync_marker == sync.bytes
        end
    end

    buffer_decoder = BinaryDecoder(IOBuffer(block_data))
    buffer_decoder, block_count
end

end
