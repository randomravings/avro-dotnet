using Avro;
using Avro.Schemas;
using NUnit.Framework;
using System;

namespace Protocols
{
    [TestFixture]
    public class ProtoclMD5Test
    {
        private const string BASE_PROTOCOL = @"{
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
                    ""errors"": [""Curse"", ""CurseMore""]
                }
            }
        }";

        private static readonly byte[] EXPECTED_MD5 = Protocol.Parse(BASE_PROTOCOL).MD5;

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
                        ""errors"": [""Curse"", ""CurseMore""]
                    }
                }
            }",
            BASE_PROTOCOL,
            TestName = "Protocol - Basic"
        )]
        public void ProtocolParseBasic(string protocolAvro, string baseProtocolAvro)
        {
            var expectedMD5 = Protocol.Parse(baseProtocolAvro).MD5;
            var actualMD5 = Protocol.Parse(protocolAvro).MD5;
            Assert.AreEqual(expectedMD5, actualMD5);
        }
    }
}
