using Avro.IO;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace Avro.File
{
    public class DataFileReader<T> : IFileReader<T>
    {
        private readonly DataFileInfo _fileHeader;
        private readonly IDatumReader<T> _datumReader;

        public DataFileReader(DataFileInfo header, IDatumReader<T> datumReader)
        {
            if (header.Schema.ToAvroCanonical() != datumReader.WriterSchema.ToAvroCanonical())
                throw new ArgumentException("Incompatible DatumReader");
            _fileHeader = header;
            _datumReader = datumReader;
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
                            yield return _datumReader.Read(decoder);
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
                case "":
                    return new MemoryStream(bytes);
                case "deflate":
                    return new DeflateStream(new MemoryStream(bytes), CompressionMode.Decompress, true);
                default:
                    throw new NotSupportedException($"Codec: '{_fileHeader.Codec}' is not supported");

            }
        }

        public void Dispose() { }
    }
}
