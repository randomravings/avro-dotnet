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

        public GenericRecord ReadRequest(IAvroDecoder decoder, string message)
        {
            return _protocolPair.RequestReaders[message].Read(decoder);
        }

        public void WriteReponse(IAvroEncoder encoder, string message, GenericResponse response) =>
            _protocolPair.ResponseWriters[message].Write(encoder, response);

        public void WriteError<T>(IAvroEncoder encoder, string message, GenericResponseError error) =>
            _protocolPair.ErrorWriters[message].Write(encoder, error);
    }
}
