using Avro.Container;
using System;
using System.IO;
using System.IO.Compression;

namespace Avro.IO
{
    public class FileWriter<T> : IAvroFileWriter<T>
    {
        private const long MAX_CHUNK_SIZE = 1073741824;

        private readonly Header _header;
        private readonly IAvroWriter<T> _writer;
        private readonly long _maxBlockCount;
        private readonly MemoryStream _serializeStream;
        private readonly IAvroEncoder _encoder;
        private readonly FileStream _fileStream;

        private long _count = 0;

        public FileWriter(Header header, IAvroWriter<T> writer, long maxBlockCount = 1000)
        {
            if (header.Schema.ToAvroCanonical() != writer.WriterSchema.ToAvroCanonical())
                throw new ArgumentException("Incompatible DatumWriter");

            _header = header;
            _writer = writer;
            _maxBlockCount = maxBlockCount;
            _serializeStream = new MemoryStream(1024 * 1024);
            _encoder = new BinaryEncoder(_serializeStream);

            _fileStream = _header.FileInfo.Open(FileMode.CreateNew, FileAccess.Write, FileShare.Read);

            using (var encoding = new BinaryEncoder(_fileStream))
            {
                encoding.WriteFixed(_header.Magic);
                encoding.WriteMap(_header.Metadata, (s, v) => s.WriteBytes(v));
                encoding.WriteFixed(_header.Sync);
            }
            _fileStream.Flush();
        }

        public void Write(T item)
        {
            _writer.Write(_encoder, item);
            _count++;
            if (_count >= _maxBlockCount || _serializeStream.Position > MAX_CHUNK_SIZE)
                WriteBlock();
        }

        private void WriteBlock()
        {
            _serializeStream.Flush();
            var data = Compress(_serializeStream.GetBuffer(), (int)_serializeStream.Position, _header.Codec);
            using (var encoding = new BinaryEncoder(_fileStream))
            {
                encoding.WriteLong(_count);
                encoding.WriteLong(data.Length);
                _fileStream.Write(data);
                encoding.WriteFixed(_header.Sync);
            }
            _count = 0;
            _serializeStream.Seek(0, SeekOrigin.Begin);
        }

        private static ReadOnlySpan<byte> Compress(byte[] data, int count, Codec? codec)
        {
            switch (codec)
            {
                case null:
                case Codec.Null:
                    return data.AsSpan(0, count);
                case Codec.Deflate:
                    var deflatedResult = new MemoryStream();
                    using (var deflater = new DeflateStream(deflatedResult, CompressionMode.Compress, true))
                    {
                        deflater.Write(data, 0, count);
                        deflater.Flush();
                    }
                    return deflatedResult.GetBuffer().AsSpan(0, (int)deflatedResult.Position);
                default:
                    throw new NotSupportedException($"Codec: '{codec}' is not supported");

            }
        }

        public void Dispose()
        {
            _fileStream.Flush();
            _fileStream.Close();
            _encoder.Dispose();
            _serializeStream.Dispose();
        }
    }
}
