using Avro.IO;
using Avro.Protocol.Schema;

namespace Avro.Protocol
{
    public sealed class GenericResponder : IAvroResponder
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

        T IAvroResponder.ReadRequest<T>(IAvroDecoder decoder, string message)
        {
            return _protocolPair.RequestReaders[message].Read(decoder) as T;
        }

        public void WriteReponse<T>(IAvroEncoder encoder, string message, T response) where T : class
        {
            _protocolPair.ResponseWriters[message].Write(encoder, response);
        }

        public void WriteError<T>(IAvroEncoder encoder, string message, T error) where T : class
        {
            _protocolPair.ErrorWriters[message].Write(encoder, error);
        }
    }
}
