using Avro.IO;
using Avro.Protocol;
using Avro.Schema;
using org.apache.avro.ipc;
using System.Collections.Generic;

namespace Avro.Ipc
{
    public abstract class Session
    {
        private static readonly BytesSchema END_OF_FRAMESCHEMA = new BytesSchema();
        private static readonly MapSchema METASCHEMA = new MapSchema(new BytesSchema());

        protected static readonly IDictionary<string, byte[]> EMPTY_META = new Dictionary<string, byte[]>();
        protected static readonly byte[] END_OF_FRAME = new byte[] { };

        protected static readonly IAvroReader<IDictionary<string, byte[]>> META_READER = new DatumReader<IDictionary<string, byte[]>>(METASCHEMA);
        protected static readonly IAvroWriter<IDictionary<string, byte[]>> META_WRITER = new DatumWriter<IDictionary<string, byte[]>>(METASCHEMA);
        protected static readonly IAvroWriter<byte[]> END_OF_FRAME_WRITER = new DatumWriter<byte[]>(END_OF_FRAMESCHEMA);

        protected static readonly IAvroReader<HandshakeRequest> HANDSHAKE_REQUEST_READER = new DatumReader<HandshakeRequest>(HandshakeRequest.SCHEMA);
        protected static readonly IAvroWriter<HandshakeRequest> HANDSHAKE_REQUEST_WRITER = new DatumWriter<HandshakeRequest>(HandshakeRequest.SCHEMA);
        protected static readonly IAvroReader<HandshakeResponse> HANDSHAKE_RESPONSE_READER = new DatumReader<HandshakeResponse>(HandshakeResponse.SCHEMA);
        protected static readonly IAvroWriter<HandshakeResponse> HANDSHAKE_RESPONSE_WRITER = new DatumWriter<HandshakeResponse>(HandshakeResponse.SCHEMA);

        protected readonly bool _stateLess;
        protected bool _handshakePending;

        protected Session(AvroProtocol protocol)
        {
            Protocol = protocol;
            RemoteProtocol = protocol;
        }

        public AvroProtocol Protocol { get; private set; } = EmptyProtocol.Value;

        public AvroProtocol RemoteProtocol { get; protected set; } = EmptyProtocol.Value;

        protected virtual bool DoHandshake() => _stateLess || _handshakePending;
    }
}
