using Avro.IO;
using Avro.Types;

namespace Avro.Protocol
{
    public interface IProtocol
    {
        AvroProtocol Protocol { get; }
        AvroProtocol RemoteProtocol { get; }

        T ReadRequest<T>(IAvroDecoder decoder, string message) where T : class, IAvroRecord;

        void WriteRequest<T>(IAvroEncoder encoder, string message, T record) where T : class, IAvroRecord;

        T ReadResponse<T>(IAvroDecoder decoder, string message) where T : class;

        void WriteReponse<T>(IAvroEncoder encoder, string message, T response) where T : class;

        T ReadError<T>(IAvroDecoder decoder, string message) where T : class;

        void WriteError<T>(IAvroEncoder encoder, string message, T error) where T : class;
    }
}
