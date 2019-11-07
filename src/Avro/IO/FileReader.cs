using Avro.Container;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;

namespace Avro.IO
{
    public class FileReader<T> : IAvroFileReader<T>
    {
        private readonly IFileInfo _file;
        private readonly Header _header;
        private readonly long _offset;
        private readonly Stream _stream;
        private readonly IAvroReader<T> _reader;

        public FileReader(IFileInfo file, AvroSchema schema)
        {
            _file = file;
            _stream = _file.Open(FileMode.Open, FileAccess.Read, FileShare.Read);
            _header = Header.FromStream(_stream);
            _offset = _stream.Position;
            _reader = new DatumReader<T>(schema, _header.Schema);
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        public async IAsyncEnumerator<ReadBlock<T>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            _stream.Seek(_offset, SeekOrigin.Begin);
            while (_stream.Position < _stream.Length)
            {
                var block = new ReadBlock<T>(_reader, _stream, _header.Codec);
                if (!block.Sync.SequenceEqual(_header.Sync))
                    throw new Exception("Sync marker mismach");
                yield return block;
            }
        }

        public IEnumerator<ReadBlock<T>> GetEnumerator()
        {
            _stream.Seek(_offset, SeekOrigin.Begin);
            while (_stream.Position < _stream.Length)
            {
                var block = new ReadBlock<T>(_reader, _stream, _header.Codec);
                if (!block.Sync.SequenceEqual(_header.Sync))
                    throw new Exception("Sync marker mismach");
                yield return block;
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
