using Avro.IO;
using Avro.Resolvers;
using System;

namespace Avro.Specific
{
    public sealed class SpecificReader<T> : IDatumReader<T>
    {
        private readonly Func<IDecoder, T> _reader;
        private readonly Action<IDecoder> _skipper;
        public SpecificReader(AvroSchema readerSchema)
            : this(readerSchema, readerSchema) { }
        public SpecificReader(AvroSchema readerSchema, AvroSchema writerSchema)
        {
            var methods = SchemaResolver.ResolveReader<T>(readerSchema, writerSchema);

            _reader = methods.Item1;
            _skipper = methods.Item2;
            ReaderSchema = readerSchema;
            WriterSchema = writerSchema;
        }
        public AvroSchema ReaderSchema { get; private set; }
        public AvroSchema WriterSchema { get; private set; }
        public T Read(IDecoder stream) => _reader.Invoke(stream);
        public T Read(IDecoder stream, ref T reuse) => reuse = _reader.Invoke(stream);
        public void Skip(IDecoder stream) => _skipper.Invoke(stream);
    }
}
