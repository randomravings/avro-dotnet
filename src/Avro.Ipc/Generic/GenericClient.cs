using Avro.Generic;
using Avro.IO;
using Avro.Ipc.IO;
using org.apache.avro.ipc;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Generic
{
    public class GenericClient : Session
    {
        private GenericProtocolPair _protocolPair;
        private Func<GenericContext, CancellationToken, Task<GenericContext>> _requestHandler;
        public GenericClient(Protocol protocol, ITranceiver tranceiver)
            : base(protocol, tranceiver)
        {
            RemoteProtocol = protocol;
            _protocolPair = GenericProtocolPair.Get(Protocol, RemoteProtocol);
            _requestHandler = RequestWithHandshake;
        }

        protected static HandshakeRequest NewHandshakeRequest(MD5 clientHash, MD5 serverHash, string clientProtocol = null)
        {
            return new HandshakeRequest()
            {
                clientHash = clientHash,
                clientProtocol = clientProtocol,
                serverHash = serverHash,
                meta = EMPTY_META
            };
        }

        public async Task<GenericContext> RequestAsync(string messageName, GenericRecord parameters, CancellationToken token)
        {
            var rpcContext = new GenericContext()
            {
                Metadata = EMPTY_META,
                MessageName = messageName,
                RequestParameters = parameters
            };
            return await _requestHandler.Invoke(rpcContext, token);
        }

        private async Task<GenericContext> RequestWithHandshake(GenericContext rpcContext, CancellationToken token)
        {
            using (var requestData = new FrameStream())
            using (var encoder = new BinaryEncoder(requestData))
            {
                rpcContext.HandshakeRequest = NewHandshakeRequest(Protocol.MD5, RemoteProtocol.MD5);
                HANDSHAKE_REQUEST_WRITER.Write(encoder, rpcContext.HandshakeRequest);
                META_WRITER.Write(encoder, EMPTY_META);
                encoder.WriteString(rpcContext.MessageName);
                _protocolPair.WriteRequest(encoder, rpcContext.MessageName, rpcContext.RequestParameters);
                encoder.WriteBytes(END_OF_FRAME);

                using (var responseData = await _tranceiver.RequestAsync(requestData, token))
                using (var decode = new BinaryDecoder(responseData))
                {
                    rpcContext.HandshakeResponse = HANDSHAKE_RESPONSE_READER.Read(decode);
                    var remoteProtocol = default(Protocol);
                    var match = rpcContext.HandshakeResponse.match;

                    if (match == HandshakeMatch.CLIENT || match == HandshakeMatch.NONE)
                    {
                        remoteProtocol = AvroReader.ReadProtocol(rpcContext.HandshakeResponse.serverProtocol);
                        _protocolPair = GenericProtocolPair.Get(Protocol, remoteProtocol);
                    }

                    if (match == HandshakeMatch.BOTH || match == HandshakeMatch.CLIENT)
                    {
                        rpcContext.Metadata = META_READER.Read(decode);
                        rpcContext.IsError = decode.ReadBoolean();
                        if (rpcContext.IsError)
                            rpcContext.Error = _protocolPair.ReadError(decode, rpcContext.MessageName);
                        else
                            rpcContext.Response = _protocolPair.ReadResponse(decode, rpcContext.MessageName);
                        _requestHandler = Request;
                    }

                    if (match == HandshakeMatch.NONE)
                    {
                        rpcContext.HandshakeRequest.serverHash = remoteProtocol.MD5;
                        rpcContext.HandshakeRequest.clientProtocol = Protocol.ToAvroCanonical();
                        _protocolPair = GenericProtocolPair.Get(Protocol, remoteProtocol);
                        rpcContext = await _requestHandler.Invoke(rpcContext, token);
                    }

                    return rpcContext;
                }
            }
        }

        private async Task<GenericContext> Request(GenericContext rpcContext, CancellationToken token)
        {
            using (var requestData = new FrameStream())
            using (var encoder = new BinaryEncoder(requestData))
            {
                META_WRITER.Write(encoder, EMPTY_META);
                encoder.WriteString(rpcContext.MessageName);
                _protocolPair.WriteRequest(encoder, rpcContext.MessageName, rpcContext.RequestParameters);
                encoder.WriteBytes(END_OF_FRAME);

                using (var responseData = await _tranceiver.RequestAsync(requestData, token))
                using (var decode = new BinaryDecoder(responseData))
                    rpcContext.Response = _protocolPair.ReadResponse(decode, rpcContext.MessageName);
            }
            return rpcContext;
        }
    }
}
