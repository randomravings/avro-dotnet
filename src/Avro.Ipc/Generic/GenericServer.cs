using Avro.IO;
using Avro.Ipc.IO;
using org.apache.avro.ipc;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Generic
{

    public class GenericServer : Session
    {
        private GenericProtocolPair _protocolPair;
        private Func<CancellationToken, Task<GenericContext>> _requestHandler;
        private Func<GenericContext, CancellationToken, Task<int>> _responseHandler;
        private Func<GenericContext, CancellationToken, Task<int>> _errorHandler;

        public GenericServer(Protocol protocol, ITranceiver tranceiver)
            : base(protocol, tranceiver)
        {
            RemoteProtocol = protocol;
            _protocolPair = GenericProtocolPair.Get(Protocol, RemoteProtocol);
            _requestHandler = ReadWithHandshakeAsync;
            _responseHandler = WriteWithHandshakeAsync;
            _errorHandler = WriteErrorWithHandshakeAsync;
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

        public async Task<GenericContext> ReceiveAsync(CancellationToken token)
        {
            return await _requestHandler.Invoke(token);
        }

        public async Task<int> RespondAsync(GenericContext rpcContext, CancellationToken token)
        {
            return await _responseHandler.Invoke(rpcContext, token);
        }

        public async Task<int> RespondErrorAsync(GenericContext rpcContext, CancellationToken token)
        {
            return await _errorHandler.Invoke(rpcContext, token);
        }

        private async Task<GenericContext> ReadAsync(CancellationToken token)
        {
            using (var requestData = await _tranceiver.ReceiveAsync(token))
            using (var decoder = new BinaryDecoder(requestData))
            {
                var metadata = META_READER.Read(decoder);
                var messageName = decoder.ReadString();
                var requestParameters = _protocolPair.ReadRequest(decoder, messageName);
                return new GenericContext()
                {
                    Metadata = metadata,
                    MessageName = messageName,
                    RequestParameters = requestParameters
                };
            }
        }

        private async Task<GenericContext> ReadWithHandshakeAsync(CancellationToken token)
        {
            using (var requestData = await _tranceiver.ReceiveAsync(token))
            using (var decoder = new BinaryDecoder(requestData))
            {
                var handshakeRequest = HANDSHAKE_REQUEST_READER.Read(decoder);
                var handshakeResponse = NewHandshakeResponse(HandshakeMatch.NONE, Protocol.MD5);

                var serverMatch = GenericProtocolPair.AreSame(Protocol.MD5, handshakeRequest.serverHash);
                var clientMatch = GenericProtocolPair.Exists(Protocol.MD5, handshakeRequest.clientHash);

                if (serverMatch && clientMatch)
                    handshakeResponse.match = HandshakeMatch.BOTH;
                else if (!serverMatch && clientMatch)
                    handshakeResponse.match = HandshakeMatch.CLIENT;
                else
                    handshakeResponse.match = HandshakeMatch.NONE;

                if (serverMatch)
                    _requestHandler = ReadAsync;

                var metadata = META_READER.Read(decoder);
                var messageName = decoder.ReadString();
                var requestParameters = _protocolPair.ReadRequest(decoder, messageName);

                return new GenericContext()
                {
                    Metadata = metadata,
                    MessageName = messageName,
                    RequestParameters = requestParameters,
                    HandshakeRequest = handshakeRequest,
                    HandshakeResponse = handshakeResponse
                };
            }
        }

        public async Task<int> WriteAsync(GenericContext rpcContext, CancellationToken token)
        {
            using (var responseData = new FrameStream())
            using (var encoder = new BinaryEncoder(responseData))
            {
                META_WRITER.Write(encoder, EMPTY_META);
                encoder.WriteBoolean(false);
                _protocolPair.WriteReponse(encoder, rpcContext.MessageName, rpcContext.Response);
                encoder.WriteBytes(END_OF_FRAME);
                return await _tranceiver.SendAsync(responseData, token);
            }
        }

        public async Task<int> WriteErrorAsync(GenericContext rpcContext, CancellationToken token)
        {
            using (var responseData = new FrameStream())
            using (var encoder = new BinaryEncoder(responseData))
            {
                META_WRITER.Write(encoder, EMPTY_META);
                encoder.WriteBoolean(true);
                _protocolPair.WriteError(encoder, rpcContext.MessageName, rpcContext.Error);
                encoder.WriteBytes(END_OF_FRAME);
                return await _tranceiver.SendAsync(responseData, token);
            }
        }

        public async Task<int> WriteWithHandshakeAsync(GenericContext rpcContext, CancellationToken token)
        {
            using (var responseData = new FrameStream())
            using (var encoder = new BinaryEncoder(responseData))
            {
                HANDSHAKE_RESPONSE_WRITER.Write(encoder, rpcContext.HandshakeResponse);
                META_WRITER.Write(encoder, EMPTY_META);
                encoder.WriteBoolean(false);
                _protocolPair.WriteReponse(encoder, rpcContext.MessageName, rpcContext.Response);
                encoder.WriteBytes(END_OF_FRAME);
                if (rpcContext.HandshakeResponse == null || rpcContext.HandshakeResponse.match != HandshakeMatch.NONE)
                {
                    _responseHandler = WriteAsync;
                    _errorHandler = WriteErrorAsync;
                }
                return await _tranceiver.SendAsync(responseData, token);
            }
        }

        public async Task<int> WriteErrorWithHandshakeAsync(GenericContext rpcContext, CancellationToken token)
        {
            using (var responseData = new FrameStream())
            using (var encoder = new BinaryEncoder(responseData))
            {
                HANDSHAKE_RESPONSE_WRITER.Write(encoder, rpcContext.HandshakeResponse);
                META_WRITER.Write(encoder, EMPTY_META);
                encoder.WriteBoolean(true);
                _protocolPair.WriteError(encoder, rpcContext.MessageName, rpcContext.Error);
                encoder.WriteBytes(END_OF_FRAME);
                if (rpcContext.HandshakeResponse == null || rpcContext.HandshakeResponse.match != HandshakeMatch.NONE)
                {
                    _responseHandler = WriteAsync;
                    _errorHandler = WriteErrorAsync;
                }
                return await _tranceiver.SendAsync(responseData, token);
            }
        }
    }
}
