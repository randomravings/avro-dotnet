using Avro.Protocol;
using org.apache.avro.ipc;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc
{

    public class GenericServer : Session
    {
        private readonly ITransportServer _server;
        private readonly GenericResponder _protocol;

        public GenericServer(AvroProtocol protocol, ITransportServer server)
            : base(protocol)
        {
            _server = server;
            _protocol = new GenericResponder(Protocol, RemoteProtocol);
        }

        public async Task<GenericContext> ReceiveAsync(CancellationToken token)
        {
            var transportContext = await _server.ReceiveAsync(token);
            var context = new GenericContext(transportContext);
            if (DoHandshake())
            {
                context.HandshakeRequest = HANDSHAKE_REQUEST_READER.Read(context.RequestDecoder);
                context.HandshakeResponse = new HandshakeResponse()
                {
                    match = HandshakeMatch.NONE,
                    serverHash = Protocol.MD5,
                    serverProtocol = string.Empty,
                    meta = EMPTY_META
                };
                var serverMatch = GenericProtocolPair.AreSame(Protocol.MD5, context.HandshakeResponse.serverHash);
                var clientMatch = GenericProtocolPair.Exists(Protocol.MD5, context.HandshakeRequest.clientHash);

                context.HandshakeResponse.match = (serverMatch, clientMatch) switch
                {
                    (true, true) => HandshakeMatch.BOTH,
                    (false, true) => HandshakeMatch.CLIENT,
                    _ => HandshakeMatch.NONE
                };

                if (context.HandshakeResponse.match == HandshakeMatch.BOTH)
                    return context;
            }

            context.RequestMetadata = context.RequestDecoder.ReadMap(d => d.ReadBytes());
            context.MessageName = context.RequestDecoder.ReadString();
            context.Parameters = _protocol.ReadRequest(context.RequestDecoder, context.MessageName);
            return context;
        }

        public async Task<int> RespondAsync(GenericContext rpcContext, CancellationToken token)
        {
            if (rpcContext.HandshakeResponse != null)
            {
                HANDSHAKE_RESPONSE_WRITER.Write(rpcContext.ResponseEncoder, rpcContext.HandshakeResponse);
                _handshakePending = rpcContext.HandshakeResponse.match == HandshakeMatch.BOTH;

                if(rpcContext.HandshakeResponse.match == HandshakeMatch.NONE)
                    return await rpcContext.DispatchAsync(token);
            }

            rpcContext.ResponseEncoder.WriteMap(rpcContext.ResponseMetadata, (e, b) => e.WriteBytes(b));
            rpcContext.ResponseEncoder.WriteBoolean(rpcContext.IsError);
            if(rpcContext.IsError)
                _protocol.WriteError<GenericResponseError>(rpcContext.ResponseEncoder, rpcContext.MessageName, rpcContext.Error);
            else
                _protocol.WriteReponse(rpcContext.ResponseEncoder, rpcContext.MessageName, rpcContext.Response);
            rpcContext.ResponseEncoder.WriteBytes(END_OF_FRAME);
            return await rpcContext.DispatchAsync(token);
        }
    }
}
