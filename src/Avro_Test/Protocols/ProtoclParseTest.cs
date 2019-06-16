using Avro;
using Avro.Schemas;
using NUnit.Framework;
using System;

namespace Protocols
{
    [TestFixture]
    public class ProtoclParseTest
    {
        [SetUp]
        public void Setup()
        {
        }

        [TestCase(
            @"{
                ""protocol"": ""TestProtocol"",
                ""namespace"": ""com.acme"",
                ""doc"": ""HelloWorld"",
                ""types"": [
                    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
                      {""name"": ""message"", ""type"": ""string""}]},
                    {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
                      {""name"": ""message"", ""type"": ""string""}]},
                    {""name"": ""CurseMore"", ""type"": ""error"", ""fields"": [
                      {""name"": ""message"", ""type"": ""string""}]}
                ],
                ""messages"": {
                    ""hello"": {
                        ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
                        ""response"": ""Greeting"",
                        ""errors"": [""Curse"", ""CurseMore""],
                        ""one-way"": false
                    }
                }
            }",
            "com.acme.TestProtocol",
            TestName = "Protocol - Basic"
        )]
        public void ProtocolParseBasic(string avroString, string expectedToString)
        {
            var protocol = Protocol.Parse(avroString);
            Assert.AreEqual(expectedToString, protocol.ToString());
        }
    }
}
