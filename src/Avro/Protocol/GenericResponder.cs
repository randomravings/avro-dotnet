using Avro.IO;
using Avro.Types;

namespace Avro.Protocol
{
    public sealed class GenericResponder
    {
        private readonly GenericProtocolPair _protocolPair;
        public GenericResponder(AvroProtocol local, AvroProtocol remote)
        {
            Local = local;
            Remote = remote;
            _protocolPair = GenericProtocolPair.Get(local, remote);
        }

        public AvroProtocol Local { get; private set; }
        public AvroProtocol Remote { get; private set; }

        public T ReadRequest<T>(IAvroDecoder decoder, string message) where T : GenericRecord
        {
            return (T)_protocolPair.RequestReaders[message].Read(decoder);
        }

        public void WriteReponse<T>(IAvroEncoder encoder, string message, T response) where T : notnull
        {
            _protocolPair.ResponseWriters[message].Write(encoder, response);
        }

        public void WriteError<T>(IAvroEncoder encoder, string message, T error) where T : notnull
        {
            _protocolPair.ErrorWriters[message].Write(encoder, error);
        }
    }
}
