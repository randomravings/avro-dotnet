using Avro.IO;
using Avro.Protocol.Schema;
using Avro.Schema;
using org.apache.avro.ipc;
using System.Collections.Generic;

namespace Avro.Ipc
{
    public abstract class Session
    {
        private static readonly BytesSchema END_OF_FRAME_SCHEMA = new BytesSchema();
        private static readonly MapSchema META_SCHEMA = new MapSchema(new BytesSchema());

        protected static readonly IDictionary<string, byte[]> EMPTY_META = new Dictionary<string, byte[]>();
        protected static readonly byte[] END_OF_FRAME = new byte[] { };

        protected static readonly IAvroReader<IDictionary<string, byte[]>> META_READER = new DatumReader<IDictionary<string, byte[]>>(META_SCHEMA);
        protected static readonly IAvroWriter<IDictionary<string, byte[]>> META_WRITER = new DatumWriter<IDictionary<string, byte[]>>(META_SCHEMA);
        protected static readonly IAvroWriter<byte[]> END_OF_FRAME_WRITER = new DatumWriter<byte[]>(END_OF_FRAME_SCHEMA);

        protected static readonly IAvroReader<HandshakeRequest> HANDSHAKE_REQUEST_READER = new DatumReader<HandshakeRequest>(HandshakeRequest._SCHEMA);
        protected static readonly IAvroWriter<HandshakeRequest> HANDSHAKE_REQUEST_WRITER = new DatumWriter<HandshakeRequest>(HandshakeRequest._SCHEMA);
        protected static readonly IAvroReader<HandshakeResponse> HANDSHAKE_RESPONSE_READER = new DatumReader<HandshakeResponse>(HandshakeResponse._SCHEMA);
        protected static readonly IAvroWriter<HandshakeResponse> HANDSHAKE_RESPONSE_WRITER = new DatumWriter<HandshakeResponse>(HandshakeResponse._SCHEMA);

        protected readonly ITranceiver _tranceiver;
        protected readonly bool _stateLess;
        protected bool _handshakePending;

        protected Session(AvroProtocol protocol, ITranceiver tranceiver)
        {
            Protocol = protocol;
            _tranceiver = tranceiver;
        }

        public AvroProtocol Protocol { get; private set; }

        public AvroProtocol RemoteProtocol { get; protected set; }

        public void Close()
        {
            _tranceiver.Close();
        }

        protected virtual bool DoHandshake()
        {
            return _stateLess || _handshakePending;

        }
    }
}
