using Avro.Container;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Text;

namespace Avro.IO
{
    public class FileWriter<T> : IAvroFileWriter<T>
    {
        private readonly object _guard = new object();
        private readonly IFileInfo _file;
        private readonly Header _header;
        private readonly IAvroWriter<T> _writer;
        private readonly long _maxBlockCount;
        private readonly Stream _stream;

        private long _count = 0;
        private WriteBlock<T> _block;

        public FileWriter(IFileInfo file, AvroSchema schema, Codec codec = Codec.Null, long maxBlockCount = 1024)
        {
            _file = file;
            _header = new Header(schema, codec);
            _stream = _file.Open(FileMode.CreateNew, FileAccess.Write, FileShare.Read);
            _maxBlockCount = maxBlockCount;
            _writer = new DatumWriter<T>(schema);
            _block = new WriteBlock<T>(_writer, codec);

            Header.WriteToStream(_stream, _header);
        }

        public void Write(WriteBlock<T> item)
        {
            lock (_guard)
                WriteBlock(_stream, item, _header.Sync);
        }

        public void Write(T item)
        {
            lock (_guard)
            {
                _block.Write(item);
                if (++_count >= _maxBlockCount)
                {
                    Flush();
                    _count = 0;
                }
            }
        }

        public void Flush()
        {
            lock (_guard)
            {
                WriteBlock(_stream, _block, _header.Sync);
                _block = new WriteBlock<T>(_writer, _header.Codec);
            }
        }

        public void Close()
        {
            lock (_guard)
                _stream.Close();
        }

        public void Dispose()
        {
            lock (_guard)
                _stream.Dispose();
        }

        private static void WriteBlock(Stream stream, WriteBlock<T> block, Sync sync)
        {
            if (block.Count == 0)
                return;
            block.Flush();
            using var encoder = new BinaryEncoder(stream, true);
            encoder.WriteLong(block.Count);
            encoder.WriteLong(block.Size);
            stream.Write(block.Data, 0, (int)block.Size);
            encoder.WriteFixed(sync);
            stream.Flush();
        }
    }
}
