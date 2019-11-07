using Avro.IO;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace Avro.Container
{
    public class ReadBlock<T> : IEnumerable<T>, IAsyncEnumerable<T>, IDisposable
    {
        private readonly IAvroReader<T> _reader;
        private readonly Codec _codec;

        private readonly byte[] _data;

        public ReadBlock(IAvroReader<T> reader, Stream stream, Codec codec)
        {
            _reader = reader;
            _codec = codec;
            using var decoder = new BinaryDecoder(stream, true);
            Count = decoder.ReadLong();
            Size = decoder.ReadLong();
            _data = new byte[Size];
            stream.Read(_data);
            stream.Read(Sync);
        }

        public long Size { get; private set; } = 0L;
        public long Count { get; private set; } = 0L;
        public byte[] Sync { get; private set; } = new byte[16];

        private static Stream CreateDataStream(byte[] data, Codec codec) => codec switch
        {
            Codec.Null => new MemoryStream(data),
            Codec.Deflate => new DeflateStream(new MemoryStream(data), CompressionMode.Decompress, false),
            _ => throw new NotSupportedException($"Codec: '{codec}' is not supported"),
        };

        public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken token = default)
        {
            using var decoder = new BinaryDecoder(CreateDataStream(_data, _codec));
            for (var i = 0L; i < Count; i++)
                yield return await _reader.ReadAsync(decoder, token);
        }

        public IEnumerator<T> GetEnumerator()
        {
            using var decoder = new BinaryDecoder(CreateDataStream(_data, _codec));
            for (var i = 0L; i < Count; i++)
                yield return _reader.Read(decoder);
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Dispose() { }
    }
}
