using Avro.Resolution;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.IO
{
    public sealed class DatumWriter<T> : IAvroWriter<T>
    {
        private readonly Action<IAvroEncoder, T> _writer;
        public DatumWriter(AvroSchema writerSchema)
        {
            _writer = SchemaResolver.ResolveWriter<T>(writerSchema);
            WriterSchema = writerSchema;
        }
        public AvroSchema WriterSchema { get; private set; }
        public void Write(IAvroEncoder stream, T value) => _writer.Invoke(stream, value);
        public async Task WriteAsync(IAvroEncoder stream, T value, CancellationToken token) => await Task.Factory.StartNew(() => _writer.Invoke(stream, value), token);
    }
}
