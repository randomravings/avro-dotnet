using Avro.IO;
using System;

namespace Avro.Specific
{
    public sealed class SpecificWriter<T> : IDatumWriter<T>
    {
        private readonly Action<IEncoder, T> _writer;
        public SpecificWriter(Schema writerSchema)
        {
            _writer = SpecificResolver.ResolveWriter<T>(writerSchema);
            WriterSchema = writerSchema;
        }
        public Schema WriterSchema { get; private set; }
        public void Write(IEncoder stream, T value) => _writer.Invoke(stream, value);
    }
}
