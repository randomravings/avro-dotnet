using System.Threading;
using System.Threading.Tasks;

namespace Avro.IO
{
    public interface IAvroWriter<T>
    {
        AvroSchema WriterSchema { get; }
        void Write(IAvroEncoder stream, T value);
        Task WriteAsync(IAvroEncoder stream, T value, CancellationToken token);
    }
}
