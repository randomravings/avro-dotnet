using Avro.Generic;
using Avro.Ipc.Generic;
using Avro.Ipc.Tcp;
using Avro.Schemas;
using Avro.Specific;
using NUnit.Framework;
using org.apache.avro.ipc;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Test.Tcp
{
    [TestFixture]
    public class SocketTest
    {
        [TestCase]
        public void TestHandshake()
        {
            var HANDSHAKE_RESPONSE_READER = new SpecificReader<HandshakeResponse>(HandshakeResponse._SCHEMA);
        }

        private const string HELLO_PROTOCOL_STRING = @"
        {
          ""namespace"": ""com.acme"",
          ""protocol"": ""HelloWorld"",
          ""doc"": ""Protocol Greetings"",

          ""types"": [
            {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
              {""name"": ""message"", ""type"": ""string""}]},
            {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
              {""name"": ""message"", ""type"": ""string""}]}
          ],

          ""messages"": {
            ""hello"": {
              ""doc"": ""Say hello."",
              ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
              ""response"": ""Greeting"",
              ""errors"": [""Curse""]
            }
          }
        }";

        private const string HELLO_MESSAGE_PARAMETERS = @"
        {
            ""name"":""com.acme"".""messages"".""hello"".""request""
            ""type"":""record"",
            ""fields"":[{""name"": ""greeting"", ""type"": ""Greeting"" }]
        }";

        [TestCase]
        public void TestHelloWorld()
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var protocol = AvroReader.ReadProtocol(HELLO_PROTOCOL_STRING);

            var server = new SocketServer("127.0.0.1", 3456);
            var hostTask = Task.Factory.StartNew(() => RunHost(server, protocol, cancellationTokenSource.Token));

            var tranceiver = SocketClient.ConnectAsync("127.0.0.1", 3456).Result;
            var client = new GenericClient(protocol, tranceiver);


            var parameterType = client.Protocol.Types.First(r => r.Name == "Greeting") as RecordSchema;
            var parameterRecordSchema = new RecordSchema(
                "hello",
                $"{client.Protocol.Namespace}.messages",
                new RecordSchema.Field[]
                {
                    new RecordSchema.Field("greeting", parameterType)
                }
            );

            var parameter = new GenericRecord(parameterType);
            parameter[0] = "Hello!";
            var parameterRecord = new GenericRecord(parameterRecordSchema);
            parameterRecord[0] = parameter;

            var rpcContext = client.RequestAsync("hello", parameterRecord, cancellationTokenSource.Token).Result;

            var responseRecord = rpcContext.Response as GenericRecord;
            Assert.AreEqual("World!", responseRecord[0]);
        }

        private async void RunHost(SocketServer host, Protocol protocol, CancellationToken token)
        {
            host.Start();
            var tranceiver = await host.ListenAsync();
            var server = new GenericServer(protocol, tranceiver);
            var rpcContext = await server.ReceiveAsync(token);
            var response = new GenericRecord(server.Protocol.Types.First(r => r.Name == "Greeting") as RecordSchema);
            response[0] = "World!";
            rpcContext.Response = response;
            await server.RespondAsync(rpcContext, token);
            server.Close();
            host.Stop();
        }
    }
}
