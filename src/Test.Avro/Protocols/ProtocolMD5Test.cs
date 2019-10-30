using Avro;
using NUnit.Framework;

namespace Test.Avro.Protocols
{
    [TestFixture]
    public class ProtocolMD5Test
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
            var expectedMD5 = AvroParser.ReadProtocol(baseProtocolAvro).MD5;
            var actualMD5 = AvroParser.ReadProtocol(protocolAvro).MD5;
            Assert.AreEqual(expectedMD5, actualMD5);
        }
    }
}
