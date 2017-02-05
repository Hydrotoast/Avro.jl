module File

using Avro.Io

export DataFileWriter,
       DataFileReader,
       append

immutable DataFileWriter
    writer::DatumWriter
end

immutable DataFileReader
    writer::DatumReader
end

append(file_writer::DataFileWriter, datum) = write(file_writer.writer, datum)

end
