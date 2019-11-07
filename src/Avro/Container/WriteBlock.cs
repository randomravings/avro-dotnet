using Avro.IO;
using System;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Container
{
    public class WriteBlock<T> : IDisposable
    {
        private readonly IAvroWriter<T> _writer;
        private readonly Codec _codec;
        private readonly MemoryStream _baseStream;
        private readonly Stream _stream;
        private readonly IAvroEncoder _encoder;
        public WriteBlock(IAvroWriter<T> writer, Codec codec)
        {
            _writer = writer;
            _codec = codec;

            _baseStream = new MemoryStream(1024 * 1024);
            _stream = CreateDataStream(_baseStream, codec);
            _encoder = new BinaryEncoder(_stream);
        }
        public long Size => _baseStream.Position;
        public long Count { get; private set; } = 0L;
        public void Write(T item)
        {
            _writer.Write(_encoder, item);
            Count++;
        }
        public byte[] Data => _baseStream.GetBuffer();
        public async void WriteAsync(T item, CancellationToken token = default) => await Task.Factory.StartNew(() => Write(item), token);
        private static Stream CreateDataStream(Stream stream, Codec codec) => codec switch
        {
            Codec.Null => stream,
            Codec.Deflate => new DeflateStream(stream, CompressionMode.Compress, true),
            _ => throw new NotSupportedException($"Codec: '{codec}' is not supported"),
        };
        public void Flush() => _stream.Flush();
        public void Close() => _stream.Close();
        public void Dispose()
        {
            _stream.Dispose();
            if (_codec == Codec.Deflate)
                _baseStream.Dispose();
            _encoder.Dispose();
        }
    }
}
