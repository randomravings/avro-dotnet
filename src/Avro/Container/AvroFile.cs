using Avro.IO;
using System.IO.Abstractions;

namespace Avro.Container
{
    public class AvroFile
    {
        private readonly IFileSystem _fileSystem;
        public AvroFile(string path)
        : this(path, new FileSystem()) { }
        public AvroFile(string path, IFileSystem fileSystem)
        {
            _fileSystem = fileSystem;
            FileInfo = _fileSystem.FileInfo.FromFileName(path);
        }
        public IFileInfo FileInfo { get; private set; }
        public IAvroFileReader<T> OpenRead<T>(AvroSchema schema) => new FileReader<T>(FileInfo, schema);
        public IAvroFileWriter<T> OpenWrite<T>(AvroSchema schema, Codec codec = Codec.Null, long maxBlockCount = 1024) => new FileWriter<T>(FileInfo, schema, codec, maxBlockCount);
    }
}
