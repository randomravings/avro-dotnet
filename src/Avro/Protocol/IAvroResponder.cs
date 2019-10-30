using Avro.IO;
using Avro.Types;

namespace Avro.Protocol
{
    public interface IAvroResponder
    {
        AvroProtocol Local { get; }
        AvroProtocol Remote { get; }
        T ReadRequest<T>(IAvroDecoder decoder, string message) where T : notnull, IAvroRecord;
        void WriteReponse<T>(IAvroEncoder encoder, string message, T response) where T : notnull;
        void WriteError<T>(IAvroEncoder encoder, string message, T error) where T : notnull;
    }
}
