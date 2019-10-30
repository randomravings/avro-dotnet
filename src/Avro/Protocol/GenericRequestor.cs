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

        public void WriteRequest<T>(IAvroEncoder encoder, string message, T record) where T : GenericRecord
        {
            _protocolPair.RequestWriters[message].Write(encoder, record);
        }

        public T ReadResponse<T>(IAvroDecoder decoder, string message) where T : class
        {
            return (T)_protocolPair.ResponseReaders[message].Read(decoder);
        }

        public T ReadError<T>(IAvroDecoder decoder, string message) where T : class
        {
            return (T)_protocolPair.ErrorReaders[message].Read(decoder);
        }
    }
}
