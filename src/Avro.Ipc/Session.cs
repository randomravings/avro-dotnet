using Avro.IO;
using Avro.Ipc.Utils;
using Avro.Schemas;
using Avro.Specific;
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

        protected static readonly IDatumReader<IDictionary<string, byte[]>> META_READER = new SpecificReader<IDictionary<string, byte[]>>(META_SCHEMA);
        protected static readonly IDatumWriter<IDictionary<string, byte[]>> META_WRITER = new SpecificWriter<IDictionary<string, byte[]>>(META_SCHEMA);
        protected static readonly IDatumWriter<byte[]> END_OF_FRAME_WRITER = new SpecificWriter<byte[]>(END_OF_FRAME_SCHEMA);

        protected static readonly IDatumReader<HandshakeRequest> HANDSHAKE_REQUEST_READER = new SpecificReader<HandshakeRequest>(HandshakeRequest._SCHEMA);
        protected static readonly IDatumWriter<HandshakeRequest> HANDSHAKE_REQUEST_WRITER = new SpecificWriter<HandshakeRequest>(HandshakeRequest._SCHEMA);
        protected static readonly IDatumReader<HandshakeResponse> HANDSHAKE_RESPONSE_READER = new SpecificReader<HandshakeResponse>(HandshakeResponse._SCHEMA);
        protected static readonly IDatumWriter<HandshakeResponse> HANDSHAKE_RESPONSE_WRITER = new SpecificWriter<HandshakeResponse>(HandshakeResponse._SCHEMA);

        protected readonly ITranceiver _tranceiver;
        protected readonly bool _stateLess;
        protected bool _handshakePending;

        protected Session(Protocol protocol, ITranceiver tranceiver)
        {
            Protocol = protocol;
            _tranceiver = tranceiver;
        }

        public Protocol Protocol { get; private set; }

        public Protocol RemoteProtocol { get; protected set; }

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
