using Avro.Resolution;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.IO
{
    public sealed class DatumReader<T> : IAvroReader<T>
    {
        private readonly Func<IAvroDecoder, T> _reader;
        private readonly Action<IAvroDecoder> _skipper;
        public DatumReader(AvroSchema readerSchema)
            : this(readerSchema, readerSchema) { }
        public DatumReader(AvroSchema readerSchema, AvroSchema writerSchema)
        {
            var methods = SchemaResolver.ResolveReader<T>(readerSchema, writerSchema);

            _reader = methods.Item1;
            _skipper = methods.Item2;
            ReaderSchema = readerSchema;
            WriterSchema = writerSchema;
        }
        public AvroSchema ReaderSchema { get; private set; }
        public AvroSchema WriterSchema { get; private set; }
        public T Read(IAvroDecoder stream) => _reader.Invoke(stream);
        public async Task<T> ReadAsync(IAvroDecoder stream, CancellationToken token) => await Task<T>.Factory.StartNew(() => _reader.Invoke(stream), token);
        public void Skip(IAvroDecoder stream) => _skipper.Invoke(stream);
        public async Task SkipAsync(IAvroDecoder stream, CancellationToken token) => await Task.Factory.StartNew(() => _skipper.Invoke(stream), token);
    }
}
