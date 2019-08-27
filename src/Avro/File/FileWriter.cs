using Avro.IO;
using System;
using System.IO;
using System.IO.Compression;

namespace Avro.File
{
    public class FileWriter<T> : IAvroFileWriter<T>
    {
        private const long MAX_CHUNK_SIZE = 1073741824;

        private readonly FileHeader _fileHeader;
        private readonly IAvroWriter<T> _datumWriter;
        private readonly long _maxBlockCount;
        private readonly MemoryStream _serializeStream;
        private readonly IAvroEncoder _encoder;
        private readonly FileStream _fileStream;

        private long _count = 0;

        public FileWriter(FileHeader header, IAvroWriter<T> datumWriter, long maxBlockCount = 1000)
        {
            if (header.Schema.ToAvroCanonical() != datumWriter.WriterSchema.ToAvroCanonical())
                throw new ArgumentException("Incompatible DatumWriter");

            _fileHeader = header;
            _datumWriter = datumWriter;
            _maxBlockCount = maxBlockCount;
            _serializeStream = new MemoryStream(1024 * 1024);
            _encoder = new BinaryEncoder(_serializeStream);

            _fileStream = _fileHeader.FileInfo.Open(FileMode.CreateNew, FileAccess.Write, FileShare.Read);

            using (var encoding = new BinaryEncoder(_fileStream))
            {
                encoding.WriteFixed(_fileHeader.Magic);
                encoding.WriteMap(_fileHeader.Metadata, (s, v) => s.WriteBytes(v));
                encoding.WriteFixed(_fileHeader.Sync);
            }
            _fileStream.Flush();
        }

        public void Write(T item)
        {
            _datumWriter.Write(_encoder, item);
            _count++;
            if (_count >= _maxBlockCount || _serializeStream.Position > MAX_CHUNK_SIZE)
                WriteBlock();
        }

        private void WriteBlock()
        {
            _serializeStream.Flush();
            var data = Compress(_serializeStream.GetBuffer(), (int)_serializeStream.Position, _fileHeader.Codec);
            using (var encoding = new BinaryEncoder(_fileStream))
            {
                encoding.WriteLong(_count);
                encoding.WriteLong(data.Length);
                _fileStream.Write(data);
                encoding.WriteFixed(_fileHeader.Sync);
            }
            _count = 0;
            _serializeStream.Seek(0, SeekOrigin.Begin);
        }

        private ReadOnlySpan<byte> Compress(byte[] data, int count, Codec? codec)
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
