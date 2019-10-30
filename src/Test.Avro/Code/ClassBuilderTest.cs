using Avro.Code;
using Avro.Schema;
using Avro.Types;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

namespace Test.Avro.Code
{
    [TestFixture]
    public class ClassBuilderTest
    {
        public class ClassBuilderTestSchema : NamedSchema { }

        [TestCase]
        public void TestUnsupportedType()
        {
            var schema = new ClassBuilderTestSchema() { Name = "X" };

            Assert.Throws(
                typeof(CodeGenException),
                () => CodeGen.Compile(Guid.NewGuid().ToString(), schema, out _)
            );
        }

        [Test, TestCaseSource(typeof(FixedSource))]
        public void FixedCode(FixedSchema fixedSchema, string expectedName, string expectedNamespace, int expectedSize, string[] expectedAliases)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), fixedSchema, out var xmlDocument);

            var type = assembly.ExportedTypes.FirstOrDefault(r => r.Name == expectedName);
            Assert.NotNull(type);
            Assert.IsTrue(type.IsClass);
            Assert.IsTrue(typeof(IAvroFixed).IsAssignableFrom(type));
            Assert.AreEqual(expectedNamespace, type.Namespace);

            var size = Convert.ToInt32(type.GetField("_SIZE")?.GetValue(null));
            Assert.AreEqual(expectedSize, size);

            var aliases = GetAliasList(xmlDocument, type.FullName ?? string.Empty, string.Empty);
            Assert.AreEqual(expectedAliases, aliases);
        }

        [Test, TestCaseSource(typeof(EnumSource))]
        public void EnumCode(EnumSchema enumSchema, string expectedName, string expectedNamespace, string[] expectedSymbols, string expectedDoc, string[] expectedAliases)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), enumSchema, out var xmlDocument);

            var type = assembly.ExportedTypes.FirstOrDefault(r => r.Name == expectedName);
            Assert.NotNull(type);
            Assert.IsTrue(type.IsEnum);
            Assert.AreEqual(expectedNamespace, type.Namespace);

            var symbols = Enum.GetNames(type);
            Assert.AreEqual(expectedSymbols, symbols);

            var doc = GetSummaryText(xmlDocument, type.FullName ?? string.Empty, string.Empty);
            Assert.AreEqual(expectedDoc, doc);

            var aliases = GetAliasList(xmlDocument, type.FullName ?? string.Empty, string.Empty);
            Assert.AreEqual(expectedAliases, aliases);
        }

        [Test, TestCaseSource(typeof(RecordSource))]
        public void RecordCode(RecordSchema recordSchema, string expectedName, string expectedNamespace, string expectedDoc, string[] expectedAliases)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), recordSchema, out var xmlDocument);

            var type = assembly.ExportedTypes.FirstOrDefault(r => r.Name == expectedName);
            Assert.NotNull(type);
            Assert.IsTrue(type.IsClass);
            Assert.IsTrue(typeof(IAvroRecord).IsAssignableFrom(type));
            Assert.AreEqual(expectedNamespace, type.Namespace);

            var doc = GetSummaryText(xmlDocument, type.FullName ?? string.Empty, string.Empty);
            Assert.AreEqual(expectedDoc, doc);

            var aliases = GetAliasList(xmlDocument, type.FullName ?? string.Empty, string.Empty);
            Assert.AreEqual(expectedAliases, aliases);
        }

        [Test, TestCaseSource(typeof(ErrorSource))]
        public void ErrorCode(ErrorSchema errorSchema, string expectedName, string expectedNamespace, string expectedDoc, string[] expectedAliases)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), errorSchema, out var xmlDocument);

            var type = assembly.ExportedTypes.FirstOrDefault(r => r.Name == expectedName);
            Assert.NotNull(type);
            Assert.IsTrue(type.IsClass);
            Assert.IsTrue(typeof(IAvroRecord).IsAssignableFrom(type));
            Assert.AreEqual(expectedNamespace, type.Namespace);

            var doc = GetSummaryText(xmlDocument, type.FullName ?? string.Empty, string.Empty);
            Assert.AreEqual(expectedDoc, doc);

            var aliases = GetAliasList(xmlDocument, type.FullName ?? string.Empty, string.Empty);
            Assert.AreEqual(expectedAliases, aliases);
        }

        [Test, TestCaseSource(typeof(RecordFieldSource))]
        public void RecordField(RecordSchema record, string expectedName, string expectedDoc, string[] expectedAliases)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), record, out var xmlDocument);

            var type = assembly.ExportedTypes.FirstOrDefault();
            Assert.NotNull(type);
            Assert.IsTrue(type.IsClass);
            Assert.IsTrue(typeof(IAvroRecord).IsAssignableFrom(type));

            var property = type.GetProperty(expectedName);
            Assert.NotNull(property);

            var doc = GetSummaryText(xmlDocument, type.FullName ?? string.Empty, property?.Name ?? string.Empty);
            Assert.AreEqual(expectedDoc, doc);

            var aliases = GetAliasList(xmlDocument, type.FullName ?? string.Empty, property?.Name ?? string.Empty);
            Assert.AreEqual(expectedAliases, aliases);
        }

        [Test, TestCaseSource(typeof(RecordFieldDefaultSource))]
        public void RecordFieldDefault(RecordSchema record, string fieldWithDefault, object expectedValue, Func<object?, object?, bool> customCompare)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), record, out _);

            var type = assembly.ExportedTypes.FirstOrDefault();
            Assert.NotNull(type);
            Assert.IsTrue(type.IsClass);
            Assert.IsTrue(typeof(IAvroRecord).IsAssignableFrom(type));

            var property = type.GetProperty(fieldWithDefault);
            Assert.NotNull(property);

            var instance = Activator.CreateInstance(type);
            var value = property?.GetValue(instance) ?? null;

            if (customCompare != null)
                Assert.True(customCompare.Invoke(value, expectedValue));
            else
                Assert.AreEqual(expectedValue, value);
        }

        private class FixedSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object?[] { new FixedSchema("Test_Name"), "Test_Name", null, 0, new string[0] };
                yield return new object?[] { new FixedSchema("Test_Namespace", "TestNamespace"), "Test_Namespace", "TestNamespace", 0, new string[0] };
                yield return new object?[] { new FixedSchema("Test_Documentation") { Size = 10 }, "Test_Documentation", null, 10, new string[0] };
            }
        }

        private class EnumSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object?[] { new EnumSchema("Test_Name"), "Test_Name", null, new string[0], string.Empty, new string[0] };
                yield return new object?[] { new EnumSchema("Test_Name", string.Empty, new string[] { "A", "C", "B" }), "Test_Name", null, new string[] { "A", "C", "B" }, string.Empty, new string[0] };
                yield return new object?[] { new EnumSchema("Test_Name", string.Empty, new string[] { "int", "Type", "B" }), "Test_Name", null, new string[] { "_int", "Type", "B" }, string.Empty, new string[0] };
                yield return new object?[] { new EnumSchema("Test_Namespace", "TestNamespace"), "Test_Namespace", "TestNamespace", new string[0], string.Empty, new string[0] };
                yield return new object?[] { new EnumSchema("Test_Documentation") { Doc = "Test Enum Documentation" }, "Test_Documentation", null, new string[0], "Test Enum Documentation", new string[0] };
            }
        }

        private class RecordSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object?[] { new RecordSchema("Test_Name"), "Test_Name", null, string.Empty, new string[0] };
                yield return new object?[] { new RecordSchema("Test_Namespace", "TestNamespace"), "Test_Namespace", "TestNamespace", string.Empty, new string[0] };
                yield return new object?[] { new RecordSchema("Test_Documentation") { Doc = "Test Record Documentation" }, "Test_Documentation", null, "Test Record Documentation", new string[0] };
                yield return new object?[] { new RecordSchema("Test_Alias") { Aliases = new List<string>() { "Key1", "Key2" } }, "Test_Alias", null, string.Empty, new string[] { "Key1", "Key2" } };
            }
        }

        private class ErrorSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object?[] { new ErrorSchema("Test_Name"), "Test_Name", null, string.Empty, new string[0] };
                yield return new object?[] { new ErrorSchema("Test_Namespace", "TestNamespace"), "Test_Namespace", "TestNamespace", string.Empty, new string[0] };
                yield return new object?[] { new ErrorSchema("Test_Documentation") { Doc = "Test Error Documentation" }, "Test_Documentation", null, "Test Error Documentation", new string[0] };
                yield return new object?[] { new ErrorSchema("Test_Alias") { Aliases = new List<string>() { "X", "Y" } }, "Test_Alias", null, string.Empty, new string[] { "X", "Y" } };
            }
        }

        private class RecordFieldSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new IntSchema()) { Doc = "Field Doc" } }, "Field1", "Field Doc", new string[0] };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field2", new StringSchema()) { Aliases = new string[] { "OldField" } } }, "Field2", string.Empty, new string[] { "OldField" } };
            }
        }

        private class RecordFieldDefaultSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new NullSchema()) { Default = JValue.CreateNull() } }, "Field1", null, null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new BooleanSchema()) { Default = true } }, "Field1", true, null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new IntSchema()) { Default = 123 } }, "Field1", 123, null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new LongSchema()) { Default = 987654321L } }, "Field1", 987654321L, null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new FloatSchema()) { Default = 98765.4321F } }, "Field1", 98765.4321F, null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new DoubleSchema()) { Default = 98765.4321D } }, "Field1", 98765.4321D, null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new BytesSchema()) { Default = @"\u0000\u0001\u0010\u00AB\u00FF" } }, "Field1", new byte[] { 0x00, 0x01, 0x10, 0xAB, 0xFF }, null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new StringSchema()) { Default = @"""Hello World!""" } }, "Field1", "Hello World!", null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new ArraySchema(new IntSchema())) { Default = JToken.Parse("[1, 2, 4]") } }, "Field1", new List<int>() { 1, 2, 4 }, null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new MapSchema(new IntSchema())) { Default = JToken.Parse(@"{""key1"":1, ""key2"":2, ""key3"":4}") } }, "Field1", new Dictionary<string, int>() { { "key1", 1 }, { "key2", 2 }, { "key3", 4 } }, null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new UuidSchema()) { Default = @"""61FEDC68-47CA-4727-BDFF-685A4E3EC846""" } }, "Field1", new Guid("61FEDC68-47CA-4727-BDFF-685A4E3EC846"), null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new UnionSchema(new NullSchema(), new IntSchema())) { Default = JValue.CreateNull() } }, "Field1", null, null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new FixedSchema("Y", string.Empty, 3)) { Default = @"\u0001\u0002\u0003" } }, "Field1", new byte[] { 0x01, 0x02, 0x03 }, null };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new EnumSchema("Y", string.Empty, new string[] { "A", "B", "C" })) { Default = @"""B""" } }, "Field1", 1, new Func<object, object, bool>((a, b) => ((int)a) == ((int)b)) };
                yield return new object?[] { new RecordSchema("X") { new RecordFieldSchema("Field1", new LogicalSchema(new IntSchema(), "ls")) { Default = 42 } }, "Field1", 42, null };
                yield return new object?[] { new RecordSchema("X") {
                        new RecordFieldSchema(
                            "Field1",
                            new RecordSchema(
                                "Y",
                                string.Empty,
                                new RecordFieldSchema[] {
                                    new RecordFieldSchema(
                                        "f1",
                                        new IntSchema()
                                    ),
                                    new RecordFieldSchema(
                                        "f2",
                                        new RecordSchema(
                                            "Z",
                                            string.Empty,
                                            new RecordFieldSchema[] {
                                                new RecordFieldSchema(
                                                    "f3",
                                                    new StringSchema()
                                                ),
                                                new RecordFieldSchema(
                                                    "f4",
                                                    new FloatSchema()
                                                )
                                            }
                                        )
                                    )
                                }
                            )
                        ) { Default = JToken.Parse(@"{""f1"":123,""f2"":{""f3"":""ABC"",""f4"":0.0012}}") }
                    },
                    "Field1",
                    null,
                    new Func<object, object, bool>(
                        (a, b) =>
                        {
                            var rec01 = (IAvroRecord)a;
                            Assert.IsNotNull(rec01);
                            Assert.AreEqual(123, rec01[0]);
                            var rec02 = (IAvroRecord?)rec01[1];
                            Assert.IsNotNull(rec02);
                            Assert.AreEqual("ABC", rec02?[0] ?? string.Empty);
                            Assert.AreEqual(0.0012F, rec02?[1] ?? float.NaN);
                            return true;
                        }
                    )
                };

            }
        }

        private static string GetSummaryText(XmlDocument doc, string typeFullName, string propertyName)
        {
            var key = propertyName == string.Empty ? $"T:{typeFullName}" : $"P:{typeFullName}.{propertyName}";
            var summaryText = doc.SelectSingleNode($"//doc/members/member[@name='{key}']/summary/text()");
            return summaryText?.InnerText.Trim(' ', '\t', '\r', '\f', '\n') ?? string.Empty;
        }

        private static string[] GetAliasList(XmlDocument doc, string typeFullName, string propertyName)
        {
            var key = propertyName == string.Empty ? $"T:{typeFullName}" : $"P:{typeFullName}.{propertyName}";
            var aliasList = doc.SelectNodes($"//doc/members/member[@name='{key}']/summary/list/listheader/term[text()='Aliases']/../../item/term/text()");
            return aliasList.OfType<XmlNode>().Select(r => r.Value).ToArray();
        }
    }
}
