module Avro

include("common.jl")
include("schema.jl")
include("io.jl")
include("generic.jl")
include("data_file.jl")

export DataFile,
       Generic,
       Io,
       Schemas

end
