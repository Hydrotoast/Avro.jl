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
    file_writer = File.create(schema, output)
    f(file_writer)
    File.close(file_writer)
end

function create_binary(f::Function, schema::Schemas.Schema, output_filename::String)
    open(output_filename, "w") do file
        create_binary(f, schema, file)
    end
end

end
