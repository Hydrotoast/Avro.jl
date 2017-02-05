module Avro

include("schema.jl")
include("io.jl")
include("file.jl")

export Io,
       Schemas,
       File

function create_binary(f::Function, schema::Schemas.Schema, output::IO)
    writer = Io.DatumWriter(Io.BinaryEncoder(output), schema)
    file_writer = File.DataFileWriter(writer)

    f(file_writer)

    close(output)
end

function create_binary(f::Function, schema::Schemas.Schema, output_filename::String)
    open(output_filename, "w") do file
        create_binary(f, schema, file)
    end
end

end
