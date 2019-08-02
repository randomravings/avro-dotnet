using Avro;
using Avro.Schemas;
using NUnit.Framework;
using System;

namespace Avro.Test.Protocols
{
    [TestFixture]
    public class ProtocolMD5Test
    {
        private static readonly byte[] EXPECTED_MD5 = AvroReader.ReadProtocol(BASE_PROTOCOL, out _).MD5;

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
                        ""response"": ""com.acme.Greeting"",
                        ""errors"": [""Curse"", ""CurseMore""]
                    }
                }
            }",
            BASE_PROTOCOL,
            TestName = "Protocol - MD5"
        )]
        public void ProtocolParseBasic(string protocolAvro, string baseProtocolAvro)
        {
            var expectedMD5 = AvroReader.ReadProtocol(baseProtocolAvro).MD5;
            var actualMD5 = AvroReader.ReadProtocol(protocolAvro).MD5;
            Assert.AreEqual(expectedMD5, actualMD5);
        }
    }
}
