using Avro.IO;
using System;

namespace Avro.Specific
{
    public sealed class SpecificReader<T> : IDatumReader<T>
    {
        private readonly Func<IDecoder, T> _reader;
        private readonly Action<IDecoder> _skipper;
        public SpecificReader(Schema readerSchema, Schema writerSchema)
        {
            var methods = SpecificResolver.ResolveReader<T>(readerSchema, writerSchema);

            _reader = methods.Item1;
            _skipper = methods.Item2;
            ReaderSchema = readerSchema;
            WriterSchema = writerSchema;
        }
        public Schema ReaderSchema { get; private set; }
        public Schema WriterSchema { get; private set; }
        public T Read(IDecoder stream) => _reader.Invoke(stream);
        public T Read(IDecoder stream, ref T reuse) => reuse = _reader.Invoke(stream);
        public void Skip(IDecoder stream) => _skipper.Invoke(stream);
    }
}
