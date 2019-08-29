using Avro.Container;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace Avro.IO
{
    public class FileReader<T> : IAvroFileReader<T>
    {
        private readonly Header _fileHeader;
        private readonly IAvroReader<T> _reader;

        public FileReader(Header header, IAvroReader<T> reader)
        {
            if (header.Schema.ToAvroCanonical() != reader.WriterSchema.ToAvroCanonical())
                throw new ArgumentException("Incompatible DatumReader");
            _fileHeader = header;
            _reader = reader;
        }

        public IEnumerator<T> GetEnumerator()
        {
            using (var stream = _fileHeader.FileInfo.Open(FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                stream.Seek(_fileHeader.FileHeaderSize, SeekOrigin.Begin);
                while (stream.Position < stream.Length)
                {
                    var blockCount = 0L;
                    var blockSize = 0L;
                    var blockData = default(byte[]);
                    var blockSync = new byte[16];
                    using (var decoder = new BinaryDecoder(stream))
                    {
                        blockCount = decoder.ReadLong();
                        blockSize = decoder.ReadLong();
                    }
                    blockData = new byte[blockSize];
                    stream.Read(blockData.AsSpan());
                    stream.Read(blockSync.AsSpan());

                    if (!blockSync.SequenceEqual(_fileHeader.Sync))
                        throw new Exception("Sync marker mismach");

                    using (var memoryStream = CreateDataStream(blockData))
                    using (var decoder = new BinaryDecoder(memoryStream))
                        for (var i = 0L; i < blockCount; i++)
                            yield return _reader.Read(decoder);
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private Stream CreateDataStream(byte[] bytes)
        {
            switch (_fileHeader.Codec)
            {
                case null:
                case Codec.Null:
                    return new MemoryStream(bytes);
                case Codec.Deflate:
                    return new DeflateStream(new MemoryStream(bytes), CompressionMode.Decompress, true);
                default:
                    throw new NotSupportedException($"Codec: '{_fileHeader.Codec}' is not supported");

            }
        }

        public void Dispose() { }
    }
}
