using Avro;
using Avro.Schema;
using NUnit.Framework;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Test.Avro.Schema
{
    [TestFixture]
    public class SchemaWriteTest
    {
        class UnknownSchema : AvroSchema { }

        [TestCase(TestName = "Unknown Schema")]
        public void UnknownSchemaTest()
        {
            var schema = new UnknownSchema();

            var canonicalAvro = new StringBuilder();
            using var writer = new StringWriter(canonicalAvro);
            Assert.Throws(typeof(AvroException), () => AvroParser.WriteAvroCanonical(writer, schema));
        }

        [Test, TestCaseSource(typeof(SchemaSource))]
        public void SchemaWrite(AvroSchema schema, string expectedCanonicalAvro, string expectedDefaultAvro, string expectedFullAvro)
        {
            var canonicalAvro = new StringBuilder();
            using (var writer = new StringWriter(canonicalAvro))
                AvroParser.WriteAvroCanonical(writer, schema);
            var actualCanonicalAvro = canonicalAvro.ToString();

            var defaultAvro = new StringBuilder();
            using (var writer = new StringWriter(defaultAvro))
                AvroParser.WriteAvro(writer, schema);
            var actualDefaultAvro = defaultAvro.ToString();

            var fullAvro = new StringBuilder();
            using (var writer = new StringWriter(fullAvro))
                AvroParser.WriteAvroFull(writer, schema);
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
                    new FieldSchema()
                    {
                        Name = "Recurse",
                        Type = _recordSchemaRecurse
                    }
                );
            }


            public IEnumerator GetEnumerator()
            {
                yield return new object[] {
                    new NullSchema(),
                    @"""null""",
                    @"""null""",
                    @"{ ""type"": ""null"" }"
                };

                yield return new object[] {
                    new BooleanSchema(),
                    @"""boolean""",
                    @"""boolean""",
                    @"{ ""type"": ""boolean"" }"
                };

                yield return new object[] {
                    new IntSchema(),
                    @"""int""",
                    @"""int""",
                    @"{ ""type"": ""int"" }"
                };

                yield return new object[] {
                    new LongSchema(),
                    @"""long""",
                    @"""long""",
                    @"{ ""type"": ""long"" }"
                };

                yield return new object[] {
                    new FloatSchema(),
                    @"""float""",
                    @"""float""",
                    @"{ ""type"": ""float"" }"
                };

                yield return new object[] {
                    new DoubleSchema(),
                    @"""double""",
                    @"""double""",
                    @"{ ""type"": ""double"" }"
                };

                yield return new object[] {
                    new BytesSchema(),
                    @"""bytes""",
                    @"""bytes""",
                    @"{ ""type"": ""bytes"" }"
                };

                yield return new object[] {
                    new StringSchema(),
                    @"""string""",
                    @"""string""",
                    @"{ ""type"": ""string"" }"
                };

                yield return new object[] {
                    new ArraySchema(new StringSchema()),
                    @"{""type"":""array"",""items"":""string""}",
                    @"{ ""type"": ""array"", ""items"": ""string"" }",
                    @"{ ""type"": ""array"", ""items"": { ""type"": ""string"" } }"
                };

                yield return new object[] {
                    new MapSchema(new DoubleSchema()),
                    @"{""type"":""map"",""values"":""double""}",
                    @"{ ""type"": ""map"", ""values"": ""double"" }",
                    @"{ ""type"": ""map"", ""values"": { ""type"": ""double"" } }"
                };

                yield return new object[] {
                    new FixedSchema()
                    {
                        Name = "TestFixed",
                        Namespace = "TestNamespace",
                        Size = 12
                    },
                    @"{""name"":""TestNamespace.TestFixed"",""type"":""fixed"",""size"":12}",
                    @"{ ""type"": ""fixed"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""size"": 12 }",
                    @"{ ""type"": ""fixed"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""size"": 12, ""aliases"": [] }"
                };

                yield return new object[] {
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
                };

                yield return new object[] {
                    new EnumSchema(
                        "TestFixed",
                        "TestNamespace",
                        new[] { "X1", "X2", "X3" }
                    ),
                    @"{""name"":""TestNamespace.TestFixed"",""type"":""enum"",""symbols"":[""X1"",""X2"",""X3""]}",
                    @"{ ""type"": ""enum"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""symbols"": [""X1"", ""X2"", ""X3""] }",
                    @"{ ""type"": ""enum"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""symbols"": [""X1"", ""X2"", ""X3""], ""doc"": """", ""aliases"": [] }"
                };

                yield return new object[] {
                    new EnumSchema(
                        "TestFixed",
                        "TestNamespace",
                        new[] { "X1", "X2", "X3" }
                    )
                    {
                        Doc = "TestDoc",
                        Aliases = new List<string>{ "A", "B" }
                    },
                    @"{""name"":""TestNamespace.TestFixed"",""type"":""enum"",""symbols"":[""X1"",""X2"",""X3""]}",
                    @"{ ""type"": ""enum"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""symbols"": [""X1"", ""X2"", ""X3""], ""doc"": ""TestDoc"", ""aliases"": [""A"", ""B""] }",
                    @"{ ""type"": ""enum"", ""name"": ""TestFixed"", ""namespace"": ""TestNamespace"", ""symbols"": [""X1"", ""X2"", ""X3""], ""doc"": ""TestDoc"", ""aliases"": [""A"", ""B""] }"
                };

                yield return new object[] {
                    new RecordSchema(
                        "TestRecord",
                        "Test.Namespace",
                        new List<FieldSchema>()
                        {
                            new FieldSchema()
                            {
                                Name = "FieldA",
                                Type = new StringSchema()
                            },
                            new FieldSchema()
                            {
                                Name = "FieldB",
                                Type = new ArraySchema(new DoubleSchema())
                            }
                        }
                    ),
                    @"{""name"":""Test.Namespace.TestRecord"",""type"":""record"",""fields"":[{""name"":""FieldA"",""type"":""string""},{""name"":""FieldB"",""type"":{""type"":""array"",""items"":""double""}}]}",
                    @"{ ""type"": ""record"", ""name"": ""TestRecord"", ""namespace"": ""Test.Namespace"", ""fields"": [{ ""name"": ""FieldA"", ""type"": ""string"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": ""double"" } }] }",
                    @"{ ""type"": ""record"", ""name"": ""TestRecord"", ""namespace"": ""Test.Namespace"", ""doc"": """", ""aliases"": [], ""fields"": [{ ""name"": ""FieldA"", ""type"": { ""type"": ""string"" }, ""default"": {}, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": { ""type"": ""double"" } }, ""default"": {}, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }] }"
                };

                yield return new object[] {
                    new RecordSchema(
                        "TestRecord",
                        "Test.Namespace",
                        new List<FieldSchema>()
                        {
                            new FieldSchema()
                            {
                                Name = "FieldA",
                                Type = new StringSchema(),
                                Default = @"""DefaultValue""",
                                Doc = "TestFieldDoc",
                                Aliases = new List<string>{ "X" },
                                Order = "ascending"
                            },
                            new FieldSchema()
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
                    @"{ ""type"": ""record"", ""name"": ""TestRecord"", ""namespace"": ""Test.Namespace"", ""doc"": ""TestDoc"", ""aliases"": [""A"", ""B""], ""fields"": [{ ""name"": ""FieldA"", ""type"": { ""type"": ""string"" }, ""default"": ""DefaultValue"", ""doc"": ""TestFieldDoc"", ""aliases"": [""X""], ""order"": ""ascending"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": { ""type"": ""double"" } }, ""default"": {}, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }] }"
                };

                yield return new object[] {
                    _recordSchemaRecurse,
                    @"{""name"":""Test.Namespace.TestRecord"",""type"":""record"",""fields"":[{""name"":""Recurse"",""type"":""Test.Namespace.TestRecord""}]}",
                    @"{ ""type"": ""record"", ""name"": ""TestRecord"", ""namespace"": ""Test.Namespace"", ""fields"": [{ ""name"": ""Recurse"", ""type"": ""Test.Namespace.TestRecord"" }] }",
                    @"{ ""type"": ""record"", ""name"": ""TestRecord"", ""namespace"": ""Test.Namespace"", ""doc"": """", ""aliases"": [], ""fields"": [{ ""name"": ""Recurse"", ""type"": ""Test.Namespace.TestRecord"", ""default"": {}, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }] }"
                };

                yield return new object[] {
                    new ErrorSchema(
                        "TestError",
                        "Test.Namespace",
                        new List<FieldSchema>()
                        {
                            new FieldSchema()
                            {
                                Name = "FieldA",
                                Type = new StringSchema()
                            },
                            new FieldSchema()
                            {
                                Name = "FieldB",
                                Type = new ArraySchema(new DoubleSchema())
                            }
                        }
                    ),
                    @"{""name"":""Test.Namespace.TestError"",""type"":""error"",""fields"":[{""name"":""FieldA"",""type"":""string""},{""name"":""FieldB"",""type"":{""type"":""array"",""items"":""double""}}]}",
                    @"{ ""type"": ""error"", ""name"": ""TestError"", ""namespace"": ""Test.Namespace"", ""fields"": [{ ""name"": ""FieldA"", ""type"": ""string"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": ""double"" } }] }",
                    @"{ ""type"": ""error"", ""name"": ""TestError"", ""namespace"": ""Test.Namespace"", ""doc"": """", ""aliases"": [], ""fields"": [{ ""name"": ""FieldA"", ""type"": { ""type"": ""string"" }, ""default"": {}, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": { ""type"": ""double"" } }, ""default"": {}, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }] }"
                };

                yield return new object[] {
                    new ErrorSchema(
                        "TestError",
                        "Test.Namespace",
                        new List<FieldSchema>()
                        {
                            new FieldSchema()
                            {
                                Name = "FieldA",
                                Type = new StringSchema(),
                                Default = @"""DefaultValue""",
                                Doc = "TestFieldDoc",
                                Aliases = new List<string>{ "X" },
                                Order = "ascending"
                            },
                            new FieldSchema()
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
                    @"{ ""type"": ""error"", ""name"": ""TestError"", ""namespace"": ""Test.Namespace"", ""doc"": ""TestDoc"", ""aliases"": [""A"", ""B""], ""fields"": [{ ""name"": ""FieldA"", ""type"": { ""type"": ""string"" }, ""default"": ""DefaultValue"", ""doc"": ""TestFieldDoc"", ""aliases"": [""X""], ""order"": ""ascending"" }, { ""name"": ""FieldB"", ""type"": { ""type"": ""array"", ""items"": { ""type"": ""double"" } }, ""default"": {}, ""doc"": """", ""aliases"": [], ""order"": ""ignore"" }] }"
                };

                yield return new object[] {
                    new DateSchema(),
                    @"{""type"":""int"",""logicalType"":""date""}",
                    @"{ ""type"": ""int"", ""logicalType"": ""date"" }",
                    @"{ ""type"": { ""type"": ""int"" }, ""logicalType"": ""date"" }"
                };

                yield return new object[] {
                    new TimeMillisSchema(),
                    @"{""type"":""int"",""logicalType"":""time-millis""}",
                    @"{ ""type"": ""int"", ""logicalType"": ""time-millis"" }",
                    @"{ ""type"": { ""type"": ""int"" }, ""logicalType"": ""time-millis"" }"
                };

                yield return new object[] {
                    new TimeMicrosSchema(),
                    @"{""type"":""long"",""logicalType"":""time-micros""}",
                    @"{ ""type"": ""long"", ""logicalType"": ""time-micros"" }",
                    @"{ ""type"": { ""type"": ""long"" }, ""logicalType"": ""time-micros"" }"
                };

                yield return new object[] {
                    new TimestampMillisSchema(),
                    @"{""type"":""long"",""logicalType"":""timestamp-millis""}",
                    @"{ ""type"": ""long"", ""logicalType"": ""timestamp-millis"" }",
                    @"{ ""type"": { ""type"": ""long"" }, ""logicalType"": ""timestamp-millis"" }"
                };

                yield return new object[] {
                    new TimestampMicrosSchema(),
                    @"{""type"":""long"",""logicalType"":""timestamp-micros""}",
                    @"{ ""type"": ""long"", ""logicalType"": ""timestamp-micros"" }",
                    @"{ ""type"": { ""type"": ""long"" }, ""logicalType"": ""timestamp-micros"" }"
                };

                yield return new object[] {
                    new DurationSchema(),
                    @"{""type"":{""name"":""duration"",""type"":""fixed"",""size"":12},""logicalType"":""duration""}",
                    @"{ ""type"": { ""type"": ""fixed"", ""name"": ""duration"", ""size"": 12 }, ""logicalType"": ""duration"" }",
                    @"{ ""type"": { ""type"": ""fixed"", ""name"": ""duration"", ""namespace"": """", ""size"": 12, ""aliases"": [] }, ""logicalType"": ""duration"" }"
                };

                yield return new object[] {
                    new UuidSchema(),
                    @"{""type"":""string"",""logicalType"":""uuid""}",
                    @"{ ""type"": ""string"", ""logicalType"": ""uuid"" }",
                    @"{ ""type"": { ""type"": ""string"" }, ""logicalType"": ""uuid"" }"
                };

                yield return new object[] {
                    new DecimalSchema() { Precision = 9, Scale = 5 },
                    @"{""type"":""bytes"",""logicalType"":""decimal"",""precision"":9,""scale"":5}",
                    @"{ ""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 9, ""scale"": 5 }",
                    @"{ ""type"": { ""type"": ""bytes"" }, ""logicalType"": ""decimal"", ""precision"": 9, ""scale"": 5 }"
                };

                yield return new object[] {
                    new LogicalSchema(new FloatSchema(), "temperature"),
                    @"{""type"":""float"",""logicalType"":""temperature""}",
                    @"{ ""type"": ""float"", ""logicalType"": ""temperature"" }",
                    @"{ ""type"": { ""type"": ""float"" }, ""logicalType"": ""temperature"" }"
                };

                yield return new object[] {
                    new UnionSchema(new NullSchema(), new IntSchema()),
                    @"[""null"",""int""]",
                    @"[""null"", ""int""]",
                    @"[{ ""type"": ""null"" }, { ""type"": ""int"" }]"
                };
            }
        }
    }
}
