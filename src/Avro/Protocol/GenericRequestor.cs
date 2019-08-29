using Avro.IO;
using Avro.Protocol.Schema;
using Avro.Types;

namespace Avro.Protocol
{
    public sealed class GenericRequestor : IAvroRequestor
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

        public void WriteRequest<T>(IAvroEncoder encoder, string message, T record) where T : class, IAvroRecord
        {
            _protocolPair.RequestWriters[message].Write(encoder, record);
        }

        public T ReadResponse<T>(IAvroDecoder decoder, string message) where T : class
        {
            return _protocolPair.ResponseReaders[message].Read(decoder) as T;
        }

        public T ReadError<T>(IAvroDecoder decoder, string message) where T : class
        {
            return _protocolPair.ErrorReaders[message].Read(decoder) as T;
        }
    }
}
