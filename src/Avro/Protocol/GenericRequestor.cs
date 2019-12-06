using Avro.IO;
using Avro.Types;

namespace Avro.Protocol
{
    public sealed class GenericRequestor
    {
        private readonly GenericProtocolPair _protocolPair;
        public GenericRequestor(AvroProtocol local, AvroProtocol remote)
        {
            Local = local;
            Remote = remote;
            _protocolPair = GenericProtocolPair.Get(local, remote);
        }

        public AvroProtocol Local { get; private set; }
        public AvroProtocol Remote { get; private set; }

        public void WriteRequest(IAvroEncoder encoder, string message, GenericRecord parameters) =>
            _protocolPair.RequestWriters[message].Write(encoder, parameters);

        public GenericResponse ReadResponse(IAvroDecoder decoder, string message) =>
            _protocolPair.ResponseReaders[message].Read(decoder);

        public GenericResponseError ReadError(IAvroDecoder decoder, string message) =>
            _protocolPair.ErrorReaders[message].Read(decoder);
    }
}
