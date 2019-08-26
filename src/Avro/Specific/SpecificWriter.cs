using Avro.IO;
using Avro.Resolvers;
using System;

namespace Avro.Specific
{
    public sealed class SpecificWriter<T> : IDatumWriter<T>
    {
        private readonly Action<IEncoder, T> _writer;
        public SpecificWriter(AvroSchema writerSchema)
        {
            _writer = SchemaResolver.ResolveWriter<T>(writerSchema);
            WriterSchema = writerSchema;
        }
        public AvroSchema WriterSchema { get; private set; }
        public void Write(IEncoder stream, T value) => _writer.Invoke(stream, value);
    }
}
