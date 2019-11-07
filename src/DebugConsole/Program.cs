using Avro.Container;
using Avro.IO;
using System.Collections.Generic;
using Test.Avro.IO.Container;
using System.Linq;
using System;
using Test.Avro.Resolution;
using Avro.Ipc.Test.Tcp;
using Avro;
using Avro.Types;
using Test.Avro.IO;
using Test.Avro.Code;
using Avro.Schema;

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
            var foo = new SocketTest();
            foo.TestHelloWorldLocal();
        }
    }
}
