using Avro.Schemas;
using NUnit.Framework;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Avro.Test.Schemas
{
    [TestFixture()]
    public class SchemaWriteTest
    {
        class UnknownSchema : Schema { }

        [TestCase(TestName = "Unknown Schema")]
        public void UnknownSchemaTest()
        {
            var schema = new UnknownSchema();

            var canonicalAvro = new StringBuilder();
            using (var writer = new StringWriter(canonicalAvro))
                Assert.Throws(typeof(AvroException), () => AvroWriter.WriteAvroCanonical(writer, schema));
        }

        [Test, TestCaseSource(typeof(SchemaSource))]
        public void SchemaWrite(Schema schema, string expectedCanonicalAvro, string expectedDefaultAvro, string expectedFullAvro)
        {
            var canonicalAvro = new StringBuilder();
            using (var writer = new StringWriter(canonicalAvro))
                AvroWriter.WriteAvroCanonical(writer, schema);
            var actualCanonicalAvro = canonicalAvro.ToString();

            var defaultAvro = new StringBuilder();
            using (var writer = new StringWriter(defaultAvro))
                AvroWriter.WriteAvro(writer, schema);
            var actualDefaultAvro = defaultAvro.ToString();

            var fullAvro = new StringBuilder();
            using (var writer = new StringWriter(fullAvro))
                AvroWriter.WriteAvroFull(writer, schema);
            var actualFullAvro = fullAvro.ToString();

            Assert.AreEqual(expectedCanonicalAvro, actualCanonicalAvro, "Canonical form mismatch");
            Assert.AreEqual(expectedDefaultAvro, actualDefaultAvro, "Default form mismatch");
            Assert.AreEqual(expectedFullAvro, actualFullAvro, "Full form mismatch");

            actualCanonicalAvro = schema.ToAvroCanonical();
            actualDefaultAvro = schema.ToAvro();
            actualFullAvro = schema.ToAvroFull();

            Assert.AreEqual(expectedCanonicalAvro, actualCanonicalAvro, "Extension - Canonical form mismatch");
            Assert.AreEqual(expectedDefaultAvro, actualDefaultAvro, "Extension - Default form mismatch");
            Assert.AreEqual(expectedFullAvro, actualFullAvro, "Extension - Full form mismatch");
        }

        class SchemaSource : IEnumerable
        {
            private readonly RecordSchema _recordSchemaRecurse = new RecordSchema()
            {
                Name = "TestRecord",
                Namespace = "Test.Namespace"
            };

            public SchemaSource()
            {
                _recordSchemaRecurse.Add(
                    new RecordSchema.Field()
                    {
                        Name = "Recurse",
                        Type = _recordSchemaRecurse
                    }
                );
            }


            public IEnumerator GetEnumerator()
            {
                yield return new TestCaseData(
                    new NullSchema(),
                    @"""null""",
                    @"""null""",
                    @"{ ""type"": ""null"" }"
                ).SetName("Null Schema");

                yield return new TestCaseData(
                    new BooleanSchema(),
                    @"""boolean""",
                    @"""boolean""",
                    @"{ ""type"": ""boolean"" }"
                ).SetName("Boolean Schema");

                yield return new TestCaseData(
                    new IntSchema(),
                    @"""int""",
                    @"""int""",
                    @"{ ""type"": ""int"" }"
                ).SetName("Int Schema");

                yield return new TestCaseData(
                    new LongSchema(),
                    @"""long""",
                    @"""long""",
                    @"{ ""type"": ""long"" }"
                ).SetName("Long Schema");

                yield return new TestCaseData(
                    new FloatSchema(),
                    @"""float""",
                    @"""float""",
                    @"{ ""type"": ""float"" }"
                ).SetName("Float Schema");

                yield return new TestCaseData(
                    new DoubleSchema(),
                    @"""double""",
                    @"""double""",
                    @"{ ""type"": ""double"" }"
                ).SetName("Double Schema");

                yield return new TestCaseData(
                    new BytesSchema(),
                    @"""bytes""",
                    @"""bytes""",
                    @"{ ""type"": ""bytes"" }"
                ).SetName("Bytes Schema");

                yield return new TestCaseData(
                    new StringSchema(),
                    @"""string""",
                    @"""string""",
                    @"{ ""type"": ""string"" }"
                ).SetName("String Schema");

                yield return new TestCaseData(
                    new ArraySchema(new StringSchema()),
                    @"{""type"":""array"",""items"":""string""}",
                    @"{ ""type"": ""array"", ""items"": ""string"" }",
                    @"{ ""type"": ""array"", ""items"": { ""type"": ""string"" } }"
                ).SetName("Array Schema");

                yield return new TestCaseData(
                    new MapSchema(new DoubleSchema()),
                    @"{""type"":""map"",""values"":""double""}",
                    @"{ ""type"": ""map"", ""values"": ""double"" }",
                    @"{ ""type"": ""map"", ""values"": { ""type"": ""double"" } }"
                ).SetName("Map Schema");

                yield return new TestCaseData(
                    new FixedSchema()
                    {
                        Name = "TestFixed",
                        Namespace = "TestNamespace",
                        Size = 12
                    },
                    @"{""name"":""TestNamespace.TestFixed"",""type"":""fixed"",""size"":12}",
                    @"{ ""type"": ""fixed"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""size"": 12 }",
                    @"{ ""type"": ""fixed"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""size"": 12, ""aliases"": [] }"
                ).SetName("Fixed Schema - Short");

                yield return new TestCaseData(
                    new FixedSchema()
                    {
                        Name = "TestFixed",
                        Namespace = "TestNamespace",
                        Size = 12,
                        Aliases = new List<string>{ "A", "B" }
                    },
                    @"{""name"":""TestNamespace.TestFixed"",""type"":""fixed"",""size"":12}",
                    @"{ ""type"": ""fixed"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""size"": 12, ""aliases"": [""A"", ""B""] }",
                    @"{ ""type"": ""fixed"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""size"": 12, ""aliases"": [""A"", ""B""] }"
                ).SetName("Fixed Schema");

                yield return new TestCaseData(
                    new EnumSchema()
                    {
                        Name = "TestFixed",
                        Namespace = "TestNamespace",
                        Symbols = new List<string>{ "X1", "X2", "X3" }
                    },
                    @"{""name"":""TestNamespace.TestFixed"",""type"":""enum"",""symbols"":[""X1"",""X2"",""X3""]}",
                    @"{ ""type"": ""enum"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""symbols"": [""X1"", ""X2"", ""X3""] }",
                    @"{ ""type"": ""enum"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""symbols"": [""X1"", ""X2"", ""X3""], ""doc"": """", ""aliases"": [] }"
                ).SetName("Enum Schema - Short");

                yield return new TestCaseData(
                    new EnumSchema()
                    {
                        Name = "TestFixed",
                        Namespace = "TestNamespace",
                        Symbols = new List<string>{ "X1", "X2", "X3" },
                        Doc = "TestDoc",
                        Aliases = new List<string>{ "A", "B" }
                    },
                    @"{""name"":""TestNamespace.TestFixed"",""type"":""enum"",""symbols"":[""X1"",""X2"",""X3""]}",
                    @"{ ""type"": ""enum"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""symbols"": [""X1"", ""X2"", ""X3""], ""doc"": ""TestDoc"", ""aliases"": [""A"", ""B""] }",
                    @"{ ""type"": ""enum"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""symbols"": [""X1"", ""X2"", ""X3""], ""doc"": ""TestDoc"", ""aliases"": [""A"", ""B""] }"
                ).SetName("Enum Schema");

                yield return new TestCaseData(
                    new RecordSchema(
                        "TestRecord",
                        "Test.Namespace",
                        new List<RecordSchema.Field>()
                        {
                            new RecordSchema.Field()
                            {
                                Name = "FieldA",
                                Type = new StringSchema()
                            },
                            new RecordSchema.Field()
                            {
                                Name = "FieldB",
                                Type = new ArraySchema(new DoubleSchema())
                            }
                        }
                    ),
                    @"{""name"":""Test.Namespace.TestRecord"",""type"":""record"",""fields"":[{""name"":""FieldA"",""type"":""string""},{""name"":""FieldB"",""type"":{""type"":""array"",""items"":""double""}}]}",
                    @"{ ""type"": ""record"", ""name"": ""TestRecord"", ""namespace"": ""Test.Namespace"", ""fields"": [{ ""name"": ""FieldA"", ""type"": ""string"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": ""double"" } }] }",
                    @"{ ""type"": ""record"", ""name"": ""TestRecord"", ""namespace"": ""Test.Namespace"", ""doc"": """", ""aliases"": [], ""fields"": [{ ""name"": ""FieldA"", ""type"": { ""type"": ""string"" }, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": { ""type"": ""double"" } }, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }] }"
                ).SetName("Record Schema - Short");

                yield return new TestCaseData(
                    new RecordSchema(
                        "TestRecord",
                        "Test.Namespace",
                        new List<RecordSchema.Field>()
                        {
                            new RecordSchema.Field()
                            {
                                Name = "FieldA",
                                Type = new StringSchema(),
                                Default = "DefaultValue",
                                Doc = "TestFieldDoc",
                                Aliases = new List<string>{ "X" },
                                Order = "ascending"
                            },
                            new RecordSchema.Field()
                            {
                                Name = "FieldB",
                                Type = new ArraySchema(new DoubleSchema())
                            }
                        }
                    )
                    {
                        Doc = "TestDoc",
                        Aliases = new List<string> { "A", "B" },
                    },
                    @"{""name"":""Test.Namespace.TestRecord"",""type"":""record"",""fields"":[{""name"":""FieldA"",""type"":""string"",""default"":""DefaultValue""},{""name"":""FieldB"",""type"":{""type"":""array"",""items"":""double""}}]}",
                    @"{ ""type"": ""record"", ""name"": ""TestRecord"", ""namespace"": ""Test.Namespace"", ""doc"": ""TestDoc"", ""aliases"": [""A"", ""B""], ""fields"": [{ ""name"": ""FieldA"", ""type"": ""string"", ""default"": ""DefaultValue"", ""doc"": ""TestFieldDoc"", ""aliases"": [""X""], ""order"": ""ascending"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": ""double"" } }] }",
                    @"{ ""type"": ""record"", ""name"": ""TestRecord"", ""namespace"": ""Test.Namespace"", ""doc"": ""TestDoc"", ""aliases"": [""A"", ""B""], ""fields"": [{ ""name"": ""FieldA"", ""type"": { ""type"": ""string"" }, ""default"": ""DefaultValue"", ""doc"": ""TestFieldDoc"", ""aliases"": [""X""], ""order"": ""ascending"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": { ""type"": ""double"" } }, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }] }"
                ).SetName("Record Schema");

                yield return new TestCaseData(
                    _recordSchemaRecurse,
                    @"{""name"":""Test.Namespace.TestRecord"",""type"":""record"",""fields"":[{""name"":""Recurse"",""type"":""Test.Namespace.TestRecord""}]}",
                    @"{ ""type"": ""record"", ""name"": ""TestRecord"", ""namespace"": ""Test.Namespace"", ""fields"": [{ ""name"": ""Recurse"", ""type"": ""Test.Namespace.TestRecord"" }] }",
                    @"{ ""type"": ""record"", ""name"": ""TestRecord"", ""namespace"": ""Test.Namespace"", ""doc"": """", ""aliases"": [], ""fields"": [{ ""name"": ""Recurse"", ""type"": ""Test.Namespace.TestRecord"", ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }] }"
                ).SetName("Record Schema - Recurse");

                yield return new TestCaseData(
                    new ErrorSchema(
                        "TestError",
                        "Test.Namespace",
                        new List<RecordSchema.Field>()
                        {
                            new RecordSchema.Field()
                            {
                                Name = "FieldA",
                                Type = new StringSchema()
                            },
                            new RecordSchema.Field()
                            {
                                Name = "FieldB",
                                Type = new ArraySchema(new DoubleSchema())
                            }
                        }
                    ),
                    @"{""name"":""Test.Namespace.TestError"",""type"":""error"",""fields"":[{""name"":""FieldA"",""type"":""string""},{""name"":""FieldB"",""type"":{""type"":""array"",""items"":""double""}}]}",
                    @"{ ""type"": ""error"", ""name"": ""TestError"", ""namespace"": ""Test.Namespace"", ""fields"": [{ ""name"": ""FieldA"", ""type"": ""string"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": ""double"" } }] }",
                    @"{ ""type"": ""error"", ""name"": ""TestError"", ""namespace"": ""Test.Namespace"", ""doc"": """", ""aliases"": [], ""fields"": [{ ""name"": ""FieldA"", ""type"": { ""type"": ""string"" }, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": { ""type"": ""double"" } }, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }] }"
                ).SetName("Error Schema - Short");

                yield return new TestCaseData(
                    new ErrorSchema(
                        "TestError",
                        "Test.Namespace",
                        new List<RecordSchema.Field>()
                        {
                            new RecordSchema.Field()
                            {
                                Name = "FieldA",
                                Type = new StringSchema(),
                                Default = "DefaultValue",
                                Doc = "TestFieldDoc",
                                Aliases = new List<string>{ "X" },
                                Order = "ascending"
                            },
                            new RecordSchema.Field()
                            {
                                Name = "FieldB",
                                Type = new ArraySchema(new DoubleSchema())
                            }
                        }
                    )
                    {
                        Doc = "TestDoc",
                        Aliases = new List<string>{ "A", "B" }
                    },
                    @"{""name"":""Test.Namespace.TestError"",""type"":""error"",""fields"":[{""name"":""FieldA"",""type"":""string"",""default"":""DefaultValue""},{""name"":""FieldB"",""type"":{""type"":""array"",""items"":""double""}}]}",
                    @"{ ""type"": ""error"", ""name"": ""TestError"", ""namespace"": ""Test.Namespace"", ""doc"": ""TestDoc"", ""aliases"": [""A"", ""B""], ""fields"": [{ ""name"": ""FieldA"", ""type"": ""string"", ""default"": ""DefaultValue"", ""doc"": ""TestFieldDoc"", ""aliases"": [""X""], ""order"": ""ascending"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": ""double"" } }] }",
                    @"{ ""type"": ""error"", ""name"": ""TestError"", ""namespace"": ""Test.Namespace"", ""doc"": ""TestDoc"", ""aliases"": [""A"", ""B""], ""fields"": [{ ""name"": ""FieldA"", ""type"": { ""type"": ""string"" }, ""default"": ""DefaultValue"", ""doc"": ""TestFieldDoc"", ""aliases"": [""X""], ""order"": ""ascending"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": { ""type"": ""double"" } }, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }] }"
                ).SetName("Error Schema");

                yield return new TestCaseData(
                    new DateSchema(),
                    @"{""type"":""int"",""logicalType"":""date""}",
                    @"{ ""type"": ""int"", ""logicalType"": ""date"" }",
                    @"{ ""type"": { ""type"": ""int"" }, ""logicalType"": ""date"" }"
                ).SetName("Date Schema");

                yield return new TestCaseData(
                    new TimeMillisSchema(),
                    @"{""type"":""int"",""logicalType"":""time-millis""}",
                    @"{ ""type"": ""int"", ""logicalType"": ""time-millis"" }",
                    @"{ ""type"": { ""type"": ""int"" }, ""logicalType"": ""time-millis"" }"
                ).SetName("Time Millis Schema");

                yield return new TestCaseData(
                    new TimeMicrosSchema(),
                    @"{""type"":""long"",""logicalType"":""time-micros""}",
                    @"{ ""type"": ""long"", ""logicalType"": ""time-micros"" }",
                    @"{ ""type"": { ""type"": ""long"" }, ""logicalType"": ""time-micros"" }"
                ).SetName("Time Micros Schema");

                yield return new TestCaseData(
                    new TimestampMillisSchema(),
                    @"{""type"":""long"",""logicalType"":""timestamp-millis""}",
                    @"{ ""type"": ""long"", ""logicalType"": ""timestamp-millis"" }",
                    @"{ ""type"": { ""type"": ""long"" }, ""logicalType"": ""timestamp-millis"" }"
                ).SetName("Timestamp Millis Schema");

                yield return new TestCaseData(
                    new TimestampMicrosSchema(),
                    @"{""type"":""long"",""logicalType"":""timestamp-micros""}",
                    @"{ ""type"": ""long"", ""logicalType"": ""timestamp-micros"" }",
                    @"{ ""type"": { ""type"": ""long"" }, ""logicalType"": ""timestamp-micros"" }"
                ).SetName("Timestamp Micros Schema");

                yield return new TestCaseData(
                    new DurationSchema(),
                    @"{""type"":{""name"":""duration"",""type"":""fixed"",""size"":12},""logicalType"":""duration""}",
                    @"{ ""type"": { ""type"": ""fixed"", ""name"": ""duration"", ""size"": 12 }, ""logicalType"": ""duration"" }",
                    @"{ ""type"": { ""type"": ""fixed"", ""name"": ""duration"", ""namespace"": """", ""size"": 12, ""aliases"": [] }, ""logicalType"": ""duration"" }"
                ).SetName("Duration Schema");

                yield return new TestCaseData(
                    new UuidSchema(),
                    @"{""type"":""string"",""logicalType"":""uuid""}",
                    @"{ ""type"": ""string"", ""logicalType"": ""uuid"" }",
                    @"{ ""type"": { ""type"": ""string"" }, ""logicalType"": ""uuid"" }"
                ).SetName("Uuid Schema");

                yield return new TestCaseData(
                    new DecimalSchema() { Precision = 9, Scale = 5 },
                    @"{""type"":""bytes"",""logicalType"":""decimal"",""precision"":9,""scale"":5}",
                    @"{ ""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 9, ""scale"": 5 }",
                    @"{ ""type"": { ""type"": ""bytes"" }, ""logicalType"": ""decimal"", ""precision"": 9, ""scale"": 5 }"
                ).SetName("Decimal Schema");

                yield return new TestCaseData(
                    new LogicalSchema(new FloatSchema(), "temperature"),
                    @"{""type"":""float"",""logicalType"":""temperature""}",
                    @"{ ""type"": ""float"", ""logicalType"": ""temperature"" }",
                    @"{ ""type"": { ""type"": ""float"" }, ""logicalType"": ""temperature"" }"
                ).SetName("Custom Logical Schema");

                yield return new TestCaseData(
                    new UnionSchema(new NullSchema(), new IntSchema()),
                    @"[""null"",""int""]",
                    @"[""null"", ""int""]",
                    @"[{ ""type"": ""null"" }, { ""type"": ""int"" }]"
                ).SetName("Union Schema");
            }
        }
    }
}
