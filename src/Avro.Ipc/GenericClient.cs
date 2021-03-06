﻿using Avro.IO;
using Avro.Ipc.IO;
using Avro.Protocol;
using Avro.Types;
using org.apache.avro.ipc;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc
{
    public class GenericClient : Session
    {
        private GenericRequestor _protocol;
        public GenericClient(AvroProtocol protocol, ITranceiver tranceiver)
            : base(protocol, tranceiver)
        {
            RemoteProtocol = protocol;
            _protocol = new GenericRequestor(Protocol, RemoteProtocol);
        }

        public async Task<GenericContext> RequestAsync(string messageName, GenericRecord parameters, CancellationToken token)
        {
            var rpcContext = new GenericContext()
            {
                Metadata = EMPTY_META,
                MessageName = messageName,
                RequestParameters = parameters
            };
            return await Request(rpcContext, token);
        }

        private async Task<GenericContext> Request(GenericContext rpcContext, CancellationToken token)
        {
            using (var requestData = new FrameStream())
            using (var encoder = new BinaryEncoder(requestData))
            {
                if (DoHandshake())
                {
                    if(rpcContext.HandshakeRequest == null)
                        rpcContext.HandshakeRequest = NewHandshakeRequest((MD5)Protocol.MD5, (MD5)RemoteProtocol.MD5);
                    HANDSHAKE_REQUEST_WRITER.Write(encoder, rpcContext.HandshakeRequest);
                }

                META_WRITER.Write(encoder, EMPTY_META);
                encoder.WriteString(rpcContext.MessageName);
                _protocol.WriteRequest(encoder, rpcContext.MessageName, rpcContext.RequestParameters);
                encoder.WriteBytes(END_OF_FRAME);
                requestData.Seek(0, SeekOrigin.Begin);

                using (var responseData = await _tranceiver.RequestAsync(rpcContext.MessageName, requestData, token))
                using (var decode = new BinaryDecoder(responseData))
                {
                    responseData.Seek(0, SeekOrigin.Begin);

                    if (DoHandshake())
                    {
                        rpcContext.HandshakeResponse = HANDSHAKE_RESPONSE_READER.Read(decode);
                        _handshakePending = rpcContext.HandshakeResponse.match == HandshakeMatch.NONE;

                        var remoteProtocol = default(AvroProtocol);
                        if (rpcContext.HandshakeResponse.match == HandshakeMatch.CLIENT || rpcContext.HandshakeResponse.match == HandshakeMatch.NONE)
                        {
                            remoteProtocol = AvroParser.ReadProtocol(rpcContext.HandshakeResponse.serverProtocol);
                            _protocol = new GenericRequestor(Protocol, remoteProtocol);
                        }

                        if (rpcContext.HandshakeResponse.match == HandshakeMatch.NONE)
                        {
                            rpcContext.HandshakeRequest.serverHash = (MD5)remoteProtocol.MD5;
                            rpcContext.HandshakeRequest.clientProtocol = Protocol.ToAvroCanonical();
                            _protocol = new GenericRequestor(Protocol, remoteProtocol);
                            rpcContext = await Request(rpcContext, token);
                        }
                    }

                    rpcContext.Metadata = META_READER.Read(decode);
                    rpcContext.IsError = decode.ReadBoolean();
                    if (rpcContext.IsError)
                        rpcContext.Error = _protocol.ReadError<object>(decode, rpcContext.MessageName);
                    else
                        rpcContext.Response = _protocol.ReadResponse<object>(decode, rpcContext.MessageName);


                    return rpcContext;
                }
            }
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
    }
}
