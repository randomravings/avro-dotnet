using Avro;
using Newtonsoft.Json;
using NUnit.Framework;
using System;

namespace Test.Avro.Protocols
{
    [TestFixture]
    public class ProtocolParseExceptionTest
    {
        [TestCase(
            @"{
                ""protocol"": ""TestProtocol"",
                ""namespace"": ""com.acme"",
                ""doc"": ""HelloWorld"",
                ""types"": [
                    {""type"": ""int""}
                ]
            }",
            typeof(AvroParseException),
            TestName = "Protocol - Int Type"
        )]
        [TestCase(
            @"{
                ""protocol"": ""TestProtocol"",
                ""namespace"": ""com.acme"",
                ""doc"": ""HelloWorld"",
                ""types"": [
                    {""name"": ""RecordType"", ""type"": ""record"", ""fields"": [{""name"": ""TestField"", ""type"": ""long""}]}
                ],
                ""messages"": {
                    ""m"": {
                        ""request"": [{""name"": ""X"", ""type"": ""Y""}],
                        ""response"": ""int""
                    }
                }
            }",
            typeof(AvroParseException),
            TestName = "Protocol - Missing Message Type"
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
                        ""response"": ""int"",
                        ""one-way"": true
                    }
                }
            }",
            typeof(AvroParseException),
            TestName = "Protocol - One Way Typed"
        )]
        [TestCase(
            @"{
                ""protocol"": ""TestProtocol"",
                ""namespace"": ""com.acme"",
                ""doc"": ""HelloWorld"",
                ""types"": [
                    {""name"": ""RecordType"", ""type"": ""record"", ""fields"": [{""name"": ""TestField"", ""type"": ""long""}]},
                    {""name"": ""ErrorType"", ""type"": ""error"", ""fields"": [{""name"": ""TestField"", ""type"": ""long""}]}
                ],
                ""messages"": {
                    ""m"": {
                        ""request"": [{""name"": ""X"", ""type"": ""RecordType""}],
                        ""response"": ""int"",
                        ""errors"": [{""key"": ""value""}]
                    }
                }
            }",
            typeof(AvroParseException),
            TestName = "Protocol - Error as Object"
        )]
        [TestCase(
            @"{
                ""protocol"": ""TestProtocol"",
                ""namespace"": ""com.acme"",
                ""doc"": ""HelloWorld"",
                ""types"": [
                    {""name"": ""RecordType"", ""type"": ""record"", ""fields"": [{""name"": ""TestField"", ""type"": ""long""}]}
                ],
                ""messages"": {
                    ""m"": {
                        ""request"": [{""name"": ""X"", ""type"": ""RecordType""}],
                        ""response"": ""int"",
                        ""errors"": [""XYZ""]
                    }
                }
            }",
            typeof(AvroParseException),
            TestName = "Protocol - Error missing"
        )]
        [TestCase(
            @"{
                ""protocol"": ""TestProtocol"",
                ""namespace"": ""com.acme"",
                ""doc"": ""HelloWorld"",
                ""types"": [
                    {""name"": ""RecordType"", ""type"": ""record"", ""fields"": [{""name"": ""TestField"", ""type"": ""long""}]}
                ],
                ""messages"": {
                    ""m"": {
                        ""request"": [{""name"": ""X"", ""type"": ""RecordType""}],
                        ""response"": ""int"",
                        ""errors"": [""RecordType""]
                    }
                }
            }",
            typeof(AvroParseException),
            TestName = "Protocol - Error not Error Type"
        )]
        public void ProtocolParseException(string avroString, Type expectedExceptionType)
        {
            Assert.Throws(
                expectedExceptionType,
                () => AvroParser.ReadProtocol(avroString)
            );
        }
    }
}
