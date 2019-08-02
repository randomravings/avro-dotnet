using Avro.IO;
using System;

namespace Avro.Generic
{
    public class GenericWriter : IDatumWriter<object>
    {
        private readonly Action<IEncoder, object> _writer;
        public GenericWriter(Schema writerSchema)
        {
            _writer = GenericResolver.ResolveWriter(writerSchema);
            WriterSchema = writerSchema;
        }
        public Schema WriterSchema { get; private set; }
        public void Write(IEncoder stream, object value) => _writer.Invoke(stream, value);
    }
}
