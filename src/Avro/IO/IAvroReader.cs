using System.Threading;
using System.Threading.Tasks;

namespace Avro.IO
{
    public interface IAvroReader<T>
    {
        AvroSchema ReaderSchema { get; }
        AvroSchema WriterSchema { get; }
        T Read(IAvroDecoder stream);
        Task<T> ReadAsync(IAvroDecoder stream, CancellationToken token);
        void Skip(IAvroDecoder stream);
        Task SkipAsync(IAvroDecoder stream, CancellationToken token);
    }
}
