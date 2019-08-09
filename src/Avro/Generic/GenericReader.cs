using Avro.IO;
using System;

namespace Avro.Generic
{
    public class GenericReader<T> : IDatumReader<T>
    {
        private readonly Func<IDecoder, object> _reader;
        private readonly Action<IDecoder> _skipper;
        public GenericReader(Schema readerSchema, Schema writerSchema)
        {
            var methods = GenericResolver.ResolveReader<T>(readerSchema, writerSchema);

            _reader = methods.Item1;
            _skipper = methods.Item2;
            ReaderSchema = readerSchema;
            WriterSchema = writerSchema;
        }
        public Schema ReaderSchema { get; private set; }
        public Schema WriterSchema { get; private set; }
        public T Read(IDecoder stream) => (T) _reader.Invoke(stream);
        public T Read(IDecoder stream, ref T reuse) => reuse = (T) _reader.Invoke(stream);
        public void Skip(IDecoder stream) => _skipper.Invoke(stream);
    }
}
