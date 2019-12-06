using Avro.IO;
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
        private readonly ITransportClient _transport;
        private GenericRequestor _protocol;
        public GenericClient(AvroProtocol protocol, ITransportClient transport)
            : base(protocol)
        {
            _transport = transport;
            _protocol = new GenericRequestor(Protocol, RemoteProtocol);
        }

        public GenericMessage Request(string messageName, GenericRecord parameters)
        {
            var rpcContext = new GenericMessage()
            {
                Metadata = EMPTY_META,
                MessageName = messageName,
                RequestParameters = parameters
            };
            return RequestInternal(rpcContext, CancellationToken.None).Result;
        }

        public async Task<GenericMessage> RequestAsync(string messageName, GenericRecord parameters, CancellationToken token)
        {
            var rpcContext = new GenericMessage()
            {
                Metadata = EMPTY_META,
                MessageName = messageName,
                RequestParameters = parameters
            };
            return await RequestInternal(rpcContext, token);
        }

        private async Task<GenericMessage> RequestInternal(GenericMessage message, CancellationToken token)
        {
            using var requestData = new FrameStream();
            using var encoder = new BinaryEncoder(requestData);

            if (DoHandshake())
            {
                if (message.HandshakeRequest == null)
                    message.HandshakeRequest = new HandshakeRequest()
                    {
                        clientHash = Protocol.MD5,
                        clientProtocol = string.Empty,
                        serverHash = RemoteProtocol.MD5,
                        meta = EMPTY_META
                    };
                HANDSHAKE_REQUEST_WRITER.Write(encoder, message.HandshakeRequest);
            }

            META_WRITER.Write(encoder, EMPTY_META);
            encoder.WriteString(message.MessageName);
            if(message.RequestParameters != GenericRecord.Empty)
                _protocol.WriteRequest(encoder, message.MessageName, message.RequestParameters);
            encoder.WriteBytes(END_OF_FRAME);
            requestData.Seek(0, SeekOrigin.Begin);

            using var responseData = await _transport.RequestAsync(message.MessageName, requestData, token);
            using var decode = new BinaryDecoder(responseData);

            responseData.Seek(0, SeekOrigin.Begin);

            if (DoHandshake())
            {
                message.HandshakeResponse = HANDSHAKE_RESPONSE_READER.Read(decode);
                _handshakePending = message.HandshakeResponse.match == HandshakeMatch.NONE;

                var remoteProtocol = AvroParser.ReadProtocol(message.HandshakeResponse.serverProtocol);
                if (message.HandshakeResponse.match == HandshakeMatch.CLIENT || message.HandshakeResponse.match == HandshakeMatch.NONE)
                    _protocol = new GenericRequestor(Protocol, remoteProtocol);

                if (message.HandshakeResponse.match == HandshakeMatch.NONE)
                {
                    message.HandshakeRequest.serverHash = remoteProtocol.MD5;
                    message.HandshakeRequest.clientProtocol = Protocol.ToAvroCanonical();
                    _protocol = new GenericRequestor(Protocol, remoteProtocol);
                    message = await RequestInternal(message, token);
                }
            }

            message.Metadata = META_READER.Read(decode);
            message.IsError = decode.ReadBoolean();
            if (message.IsError)
                message.Error = _protocol.ReadError(decode, message.MessageName);
            else
                message.Response = _protocol.ReadResponse(decode, message.MessageName);
            return message;
        }
    }
}
