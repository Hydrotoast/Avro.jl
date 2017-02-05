module Avro

include("common.jl")
include("schema.jl")
include("io.jl")
include("generic.jl")
include("file.jl")

export Schemas,
       Io,
       Generic,
       File

function create_binary(f::Function, schema::Schemas.Schema, output::IO)
    writer = Io.DatumWriter(Io.BinaryEncoder(output), schema)
    file_writer = File.DataFileWriter(writer)
    File.write_header(file_writer)

    f(file_writer)

    close(output)
end

function create_binary(f::Function, schema::Schemas.Schema, output_filename::String)
    open(output_filename, "w") do file
        create_binary(f, schema, file)
    end
end

end
