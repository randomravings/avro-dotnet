using Avro.Resolution;
using System;

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
    }
}
