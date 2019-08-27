using Avro;
using Avro.Schema;
using NUnit.Framework;
using System;

namespace Avro.Test.Protocols
{
    [TestFixture]
    public class ProtocolParseTest
    {
        [TestCase(
            @"{
                ""protocol"": ""TestProtocol""
            }",
            "TestProtocol",
            TestName = "Protocol - Minimal"
        )]
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
                        ""doc"": ""Test Documentation"",
                        ""one-way"": false
                    }
                }
            }",
            "com.acme.TestProtocol",
            TestName = "Protocol - Basic"
        )]
        [TestCase(
            @"{
                ""protocol"": ""TestProtocol"",
                ""namespace"": ""com.acme"",
                ""doc"": ""HelloWorld"",
                ""types"": [
                    {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
                      {""name"": ""message"", ""type"": ""string""}]},
                ],
                ""messages"": {
                    ""hello"": {
                        ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
                        ""response"": ""null"",
                        ""one-way"": true
                    }
                }
            }",
            "com.acme.TestProtocol",
            TestName = "Protocol - One Way"
        )]
        public void ProtocolParseBasic(string avroString, string expectedToString)
        {
            var protocol = AvroProtocol.Parse(avroString);
            Assert.AreEqual(expectedToString, protocol.ToString());
        }
    }
}
