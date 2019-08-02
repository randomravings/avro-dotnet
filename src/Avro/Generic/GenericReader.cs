using Avro.IO;
using System;

namespace Avro.Generic
{
    public class GenericReader : IDatumReader<object>
    {
        private readonly Func<IDecoder, object> _reader;
        private readonly Action<IDecoder> _skipper;
        public GenericReader(Schema readerSchema, Schema writerSchema)
        {
            var methods = GenericResolver.ResolveReader(readerSchema, writerSchema);

            _reader = methods.Item1;
            _skipper = methods.Item2;
            ReaderSchema = readerSchema;
            WriterSchema = writerSchema;
        }
        public Schema ReaderSchema { get; private set; }
        public Schema WriterSchema { get; private set; }
        public object Read(IDecoder stream) => _reader.Invoke(stream);
        public object Read(IDecoder stream, ref object reuse) => reuse = _reader.Invoke(stream);
        public void Skip(IDecoder stream) => _skipper.Invoke(stream);
    }
}
