module Avro

include("common.jl")
include("schema.jl")
include("io.jl")
include("generic.jl")
include("file_common.jl")
include("file_writer.jl")
include("file_reader.jl")

export FileWriter,
       FileReader
       Generic,
       Io,
       Schemas

end
