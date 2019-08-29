using Avro.IO;
using Avro.Ipc.IO;
using Avro.Protocol;
using Avro.Protocol.Schema;
using Avro.Types;
using org.apache.avro.ipc;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc
{

    public class GenericServer : Session
    {
        private IAvroResponder _protocol;

        public GenericServer(AvroProtocol protocol, ITranceiver tranceiver)
            : base(protocol, tranceiver)
        {
            RemoteProtocol = protocol;
            _protocol = new GenericResponder(Protocol, RemoteProtocol);
        }

        public async Task<GenericContext> ReceiveAsync(CancellationToken token)
        {
            var context = new GenericContext();
            using (var requestData = await _tranceiver.ReceiveAsync(token))
            using (var decoder = new BinaryDecoder(requestData))
            {
                requestData.Seek(0, SeekOrigin.Begin);
                if (DoHandshake())
                {
                    var handshakeRequest = HANDSHAKE_REQUEST_READER.Read(decoder);
                    context.HandshakeResponse = NewHandshakeResponse(HandshakeMatch.NONE, (MD5)Protocol.MD5);
                    var serverMatch = GenericProtocolPair.AreSame(Protocol.MD5, context.HandshakeResponse.serverHash);
                    var clientMatch = GenericProtocolPair.Exists(Protocol.MD5, handshakeRequest.clientHash);

                    if (serverMatch && clientMatch)
                        context.HandshakeResponse.match = HandshakeMatch.BOTH;
                    else if (!serverMatch && clientMatch)
                        context.HandshakeResponse.match = HandshakeMatch.CLIENT;
                    else
                        context.HandshakeResponse.match = HandshakeMatch.NONE;
                }
                context.Metadata = META_READER.Read(decoder);
                context.MessageName = decoder.ReadString();
                context.RequestParameters = _protocol.ReadRequest<GenericRecord>(decoder, context.MessageName);
                return context;
            }
        }

        public async Task<int> RespondAsync(GenericContext rpcContext, CancellationToken token)
        {
            using (var responseData = new FrameStream())
            using (var encoder = new BinaryEncoder(responseData))
            {
                if (DoHandshake())
                {
                    HANDSHAKE_RESPONSE_WRITER.Write(encoder, rpcContext.HandshakeResponse);
                    _handshakePending = rpcContext.HandshakeResponse.match == HandshakeMatch.BOTH;
                }
                META_WRITER.Write(encoder, EMPTY_META);
                encoder.WriteBoolean(rpcContext.IsError);
                if (rpcContext.IsError)
                    _protocol.WriteError(encoder, rpcContext.MessageName, rpcContext.Error);
                else
                    _protocol.WriteReponse(encoder, rpcContext.MessageName, rpcContext.Response);
                encoder.WriteBytes(END_OF_FRAME);
                responseData.Seek(0, SeekOrigin.Begin);
                return await _tranceiver.SendAsync(responseData, token);
            }
        }

        protected static HandshakeResponse NewHandshakeResponse(HandshakeMatch match, MD5 serverHash, string serverProtocol = null)
        {
            return new HandshakeResponse()
            {
                match = match,
                serverHash = serverHash,
                serverProtocol = serverProtocol,
                meta = EMPTY_META
            };
        }
    }
}
