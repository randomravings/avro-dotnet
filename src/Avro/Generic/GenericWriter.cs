using Avro.IO;
using System;

namespace Avro.Generic
{
    public class GenericWriter<T> : IDatumWriter<T>
    {
        private readonly Action<IEncoder, dynamic> _writer;
        public GenericWriter(AvroSchema writerSchema)
        {
            _writer = GenericResolver.ResolveWriter<T>(writerSchema);
            WriterSchema = writerSchema;
        }
        public AvroSchema WriterSchema { get; private set; }
        public void Write(IEncoder stream, T value) => _writer.Invoke(stream, value);
    }
}
