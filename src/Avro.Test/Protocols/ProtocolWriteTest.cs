using Avro;
using Avro.Protocol;
using Avro.Protocol.Schema;
using Avro.Schema;
using NUnit.Framework;
using System.Collections;
using System.IO;
using System.Text;

namespace Avro.Test.Protocols
{
    [TestFixture]
    public class ProtocolWriteTest
    {
        [Test, TestCaseSource(typeof(ProtocolSource))]
        public void ProtocolWrite(AvroProtocol protocol, string expectedCanonicalAvro, string expectedDefaultAvro, string expectedFullAvro)
        {
            var canonicalAvro = new StringBuilder();
            using (var writer = new StringWriter(canonicalAvro))
                AvroParser.WriteAvroCanonical(writer, protocol);
            var actualCanonicalAvro = canonicalAvro.ToString();

            var defaultAvro = new StringBuilder();
            using (var writer = new StringWriter(defaultAvro))
                AvroParser.WriteAvro(writer, protocol);
            var actualDefaultAvro = defaultAvro.ToString();

            var fullAvro = new StringBuilder();
            using (var writer = new StringWriter(fullAvro))
                AvroParser.WriteAvroFull(writer, protocol);
            var actualFullAvro = fullAvro.ToString();

            Assert.AreEqual(expectedCanonicalAvro, actualCanonicalAvro, "Canonical form mismatch");
            Assert.AreEqual(expectedDefaultAvro, actualDefaultAvro, "Default form mismatch");
            Assert.AreEqual(expectedFullAvro, actualFullAvro, "Full form mismatch");

            actualCanonicalAvro = protocol.ToAvroCanonical();
            actualDefaultAvro = protocol.ToAvro();
            actualFullAvro = protocol.ToAvroFull();

            Assert.AreEqual(expectedCanonicalAvro, actualCanonicalAvro, "Extension - Canonical form mismatch");
            Assert.AreEqual(expectedDefaultAvro, actualDefaultAvro, "Extension - Default form mismatch");
            Assert.AreEqual(expectedFullAvro, actualFullAvro, "Extension - Full form mismatch");
        }

        class ProtocolSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                var record01 = new RecordSchema()
                {
                    Name = "TestRecord01"
                };
                var record02 = new RecordSchema()
                {
                    Name = "TestRecord02"
                };

                var errord01 = new ErrorSchema()
                {
                    Name = "TestError01"
                };
                var errord02 = new ErrorSchema()
                {
                    Name = "TestError02"
                };


                var protocol01 = new AvroProtocol
                {
                    Name = "ProtocolName"
                };

                yield return new object[] {
                    protocol01,
                    @"{""protocol"":""ProtocolName""}",
                    @"{ ""protocol"": ""ProtocolName"" }",
                    @"{ ""namespace"": """", ""protocol"": ""ProtocolName"", ""doc"": """", ""types"": [], ""messages"": {} }"
                };

                var protocol02 = new AvroProtocol
                {
                    Name = "ProtocolName",
                    Namespace = "Test.Namespace"
                };

                yield return new object[] {
                    protocol02,
                    @"{""protocol"":""Test.Namespace.ProtocolName""}",
                    @"{ ""namespace"": ""Test.Namespace"", ""protocol"": ""ProtocolName"" }",
                    @"{ ""namespace"": ""Test.Namespace"", ""protocol"": ""ProtocolName"", ""doc"": """", ""types"": [], ""messages"": {} }"
                };

                var protocol03 = new AvroProtocol
                {
                    Name = "ProtocolName",
                    Namespace = "Test.Namespace",
                    Doc = "Test Documentation"
                };

                yield return new object[] {
                    protocol03,
                    @"{""protocol"":""Test.Namespace.ProtocolName""}",
                    @"{ ""namespace"": ""Test.Namespace"", ""protocol"": ""ProtocolName"", ""doc"": ""Test Documentation"" }",
                    @"{ ""namespace"": ""Test.Namespace"", ""protocol"": ""ProtocolName"", ""doc"": ""Test Documentation"", ""types"": [], ""messages"": {} }"
                };

                var protocol04 = new AvroProtocol
                {
                    Name = "ProtocolName",
                    Namespace = "Test.Namespace",
                    Doc = "Test Documentation"
                };

                protocol04.AddType(record01);
                protocol04.AddType(record02);
                protocol04.AddType(errord01);
                protocol04.AddType(errord02);

                yield return new object[] {
                    protocol04,
                    @"{""protocol"":""Test.Namespace.ProtocolName"",""types"":" +
                    @"[{""name"":""TestRecord01"",""type"":""record"",""fields"":[]}," +
                        @"{""name"":""TestRecord02"",""type"":""record"",""fields"":[]}," +
                        @"{""name"":""TestError01"",""type"":""error"",""fields"":[]}," +
                        @"{""name"":""TestError02"",""type"":""error"",""fields"":[]}" +
                    @"]}",

                    @"{ ""namespace"": ""Test.Namespace"", ""protocol"": ""ProtocolName"", ""doc"": ""Test Documentation"", ""types"": " +
                    @"[{ ""type"": ""record"", ""name"": ""TestRecord01"", ""fields"": [] }, " +
                        @"{ ""type"": ""record"", ""name"": ""TestRecord02"", ""fields"": [] }, " +
                        @"{ ""type"": ""error"", ""name"": ""TestError01"", ""fields"": [] }, " +
                        @"{ ""type"": ""error"", ""name"": ""TestError02"", ""fields"": [] }" +
                    @"] }",

                    @"{ ""namespace"": ""Test.Namespace"", ""protocol"": ""ProtocolName"", ""doc"": ""Test Documentation"", ""types"": " +
                    @"[{ ""type"": ""record"", ""name"": ""TestRecord01"", ""namespace"": """", ""doc"": """", ""aliases"": [], ""fields"": [] }, " +
                        @"{ ""type"": ""record"", ""name"": ""TestRecord02"", ""namespace"": """", ""doc"": """", ""aliases"": [], ""fields"": [] }, " +
                        @"{ ""type"": ""error"", ""name"": ""TestError01"", ""namespace"": """", ""doc"": """", ""aliases"": [], ""fields"": [] }, " +
                        @"{ ""type"": ""error"", ""name"": ""TestError02"", ""namespace"": """", ""doc"": """", ""aliases"": [], ""fields"": [] " +
                    @"}]," +
                    @" ""messages"": {} }"
                };

                var protocol05 = new AvroProtocol
                {
                    Name = "ProtocolName",
                    Namespace = "Test.Namespace",
                    Doc = "Test Documentation"
                };

                protocol05.AddType(record01);
                protocol05.AddType(record02);
                protocol05.AddType(errord01);
                protocol05.AddType(errord02);

                var message01 = new MessageSchema("M01") { Doc = "Test Doc 01" };
                message01.AddParameter(new ParameterSchema("p01", record01.FullName));
                message01.AddParameter(new ParameterSchema("p02", record02.FullName));
                message01.AddError(errord01);
                message01.AddError(errord02);
                message01.Response = new DoubleSchema();

                var message02 = new MessageSchema("M02");
                message02.AddParameter(new ParameterSchema("p01", record01.FullName));
                message02.AddError(errord01);
                message02.Oneway = true;

                protocol05.AddMessage(message01);
                protocol05.AddMessage(message02);

                yield return new object[] {
                    protocol05,
                    @"{""protocol"":""Test.Namespace.ProtocolName"",""types"":" +
                    @"[{""name"":""TestRecord01"",""type"":""record"",""fields"":[]}," +
                        @"{""name"":""TestRecord02"",""type"":""record"",""fields"":[]}," +
                        @"{""name"":""TestError01"",""type"":""error"",""fields"":[]}," +
                        @"{""name"":""TestError02"",""type"":""error"",""fields"":[]}" +
                    @"]," +
                    @"""messages"":{" +
                    @"""M01"":{" +
                        @"""request"":[{""name"":""p01"",""type"":""TestRecord01""},{""name"":""p02"",""type"":""TestRecord02""}]," +
                        @"""response"":""double""," +
                        @"""errors"":[""TestError01"",""TestError02""]" +
                    @"}," +
                    @"""M02"":{" +
                        @"""request"":[{""name"":""p01"",""type"":""TestRecord01""}]," +
                        @"""errors"":[""TestError01""]," +
                        @"""one-way"":true" +
                    @"}}}",

                    @"{ ""namespace"": ""Test.Namespace"", ""protocol"": ""ProtocolName"", ""doc"": ""Test Documentation"", ""types"": " +
                    @"[{ ""type"": ""record"", ""name"": ""TestRecord01"", ""fields"": [] }, " +
                        @"{ ""type"": ""record"", ""name"": ""TestRecord02"", ""fields"": [] }, " +
                        @"{ ""type"": ""error"", ""name"": ""TestError01"", ""fields"": [] }, " +
                        @"{ ""type"": ""error"", ""name"": ""TestError02"", ""fields"": [] }" +
                    @"], " +
                    @"""messages"": {" +
                    @"""M01"": {" +
                        @"""doc"": ""Test Doc 01"", " +
                        @"""request"": [{ ""name"": ""p01"", ""type"": ""TestRecord01"" }, { ""name"": ""p02"", ""type"": ""TestRecord02"" }], " +
                        @"""response"": ""double"", " +
                        @"""errors"": [""TestError01"", ""TestError02""]" +
                    @"}, " +
                    @"""M02"": {" +
                        @"""request"": [{ ""name"": ""p01"", ""type"": ""TestRecord01"" }], " +
                        @"""errors"": [""TestError01""], " +
                        @"""one-way"": true" +
                    @"}} }",

                    @"{ ""namespace"": ""Test.Namespace"", ""protocol"": ""ProtocolName"", ""doc"": ""Test Documentation"", ""types"": " +
                    @"[{ ""type"": ""record"", ""name"": ""TestRecord01"", ""namespace"": """", ""doc"": """", ""aliases"": [], ""fields"": [] }, " +
                        @"{ ""type"": ""record"", ""name"": ""TestRecord02"", ""namespace"": """", ""doc"": """", ""aliases"": [], ""fields"": [] }, " +
                        @"{ ""type"": ""error"", ""name"": ""TestError01"", ""namespace"": """", ""doc"": """", ""aliases"": [], ""fields"": [] }, " +
                        @"{ ""type"": ""error"", ""name"": ""TestError02"", ""namespace"": """", ""doc"": """", ""aliases"": [], ""fields"": [] }" +
                    @"], " +
                    @"""messages"": {" +
                    @"""M01"": {" +
                        @"""doc"": ""Test Doc 01"", " +
                        @"""request"": [{ ""name"": ""p01"", ""type"": ""TestRecord01"" }, { ""name"": ""p02"", ""type"": ""TestRecord02"" }], " +
                        @"""response"": { ""type"": ""double"" }, " +
                        @"""errors"": [""TestError01"", ""TestError02""], " +
                        @"""one-way"": false" +
                    @"}, " +
                    @"""M02"": {" +
                        @"""doc"": """", " +
                        @"""request"": [{ ""name"": ""p01"", ""type"": ""TestRecord01"" }], " +
                        @"""response"": null, " +
                        @"""errors"": [""TestError01""], " +
                        @"""one-way"": true" +
                    @"}} }"
                };
            }
        }
    }
}
