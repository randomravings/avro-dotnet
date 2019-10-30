using Avro.IO;
using Avro.Protocol.Schema;
using Avro.Types;

namespace Avro.Protocol
{
    public interface IAvroRequestor
    {
        AvroProtocol Local { get; }
        AvroProtocol Remote { get; }
        void WriteRequest<T>(IAvroEncoder encoder, string message, T record) where T : class;
        T ReadResponse<T>(IAvroDecoder decoder, string message) where T : class;
        T ReadError<T>(IAvroDecoder decoder, string message) where T : class;
    }
}
