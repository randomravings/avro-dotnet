using Avro;
using Avro.Ipc.Test.Tcp;
using Avro.Schema;
using Newtonsoft.Json.Linq;
using Test.Avro.Code;
using Test.Avro.IO;
using Test.Avro.IO.Container;
using Test.Avro.Resolution;

namespace DebugConsole
{
    class Program
    {
        private static readonly AvroProtocol HELLO_PROTOCOL = AvroParser.ReadProtocol(@"
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
        }");

        static void Main()
        {
            var foo = new JsonEncoderTest();
            foo.TestSimple();
        }
    }
}
