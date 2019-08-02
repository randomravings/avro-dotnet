using Avro.Code;
using Avro.Schemas;
using Avro.Specific;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

namespace Avro.Test.Code
{
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
            Assert.IsTrue(typeof(ISpecificFixed).IsAssignableFrom(type));
            Assert.AreEqual(expectedNamespace, type.Namespace);

            var size = Convert.ToInt32(type.GetField("_SIZE").GetValue(null));
            Assert.AreEqual(expectedSize, size);

            var aliases = GetAliasList(xmlDocument, type.FullName);
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

            var doc = GetSummaryText(xmlDocument, type.FullName);
            Assert.AreEqual(expectedDoc, doc);

            var aliases = GetAliasList(xmlDocument, type.FullName);
            Assert.AreEqual(expectedAliases, aliases);
        }

        [Test, TestCaseSource(typeof(RecordSource))]
        public void RecordCode(RecordSchema recordSchema, string expectedName, string expectedNamespace, string expectedDoc, string[] expectedAliases)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), recordSchema, out var xmlDocument);

            var type = assembly.ExportedTypes.FirstOrDefault(r => r.Name == expectedName);
            Assert.NotNull(type);
            Assert.IsTrue(type.IsClass);
            Assert.IsTrue(typeof(ISpecificRecord).IsAssignableFrom(type));
            Assert.AreEqual(expectedNamespace, type.Namespace);

            var doc = GetSummaryText(xmlDocument, type.FullName);
            Assert.AreEqual(expectedDoc, doc);

            var aliases = GetAliasList(xmlDocument, type.FullName);
            Assert.AreEqual(expectedAliases, aliases);
        }

        [Test, TestCaseSource(typeof(ErrorSource))]
        public void ErrorCode(ErrorSchema errorSchema, string expectedName, string expectedNamespace, string expectedDoc, string[] expectedAliases)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), errorSchema, out var xmlDocument);

            var type = assembly.ExportedTypes.FirstOrDefault(r => r.Name == expectedName);
            Assert.NotNull(type);
            Assert.IsTrue(type.IsClass);
            Assert.IsTrue(typeof(ISpecificRecord).IsAssignableFrom(type));
            Assert.AreEqual(expectedNamespace, type.Namespace);

            var doc = GetSummaryText(xmlDocument, type.FullName);
            Assert.AreEqual(expectedDoc, doc);

            var aliases = GetAliasList(xmlDocument, type.FullName);
            Assert.AreEqual(expectedAliases, aliases);
        }

        [Test, TestCaseSource(typeof(RecordFieldSource))]
        public void RecordField(RecordSchema record, string expectedName, string expectedDoc, string[] expectedAliases)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), record, out var xmlDocument);

            var type = assembly.ExportedTypes.FirstOrDefault();
            Assert.NotNull(type);
            Assert.IsTrue(type.IsClass);
            Assert.IsTrue(typeof(ISpecificRecord).IsAssignableFrom(type));

            var property = type.GetProperty(expectedName);
            Assert.NotNull(type);

            var doc = GetSummaryText(xmlDocument, type.FullName, property.Name);
            Assert.AreEqual(expectedDoc, doc);

            var aliases = GetAliasList(xmlDocument, type.FullName, property.Name);
            Assert.AreEqual(expectedAliases, aliases);
        }

        private class FixedSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new FixedSchema("Test_Name"), "Test_Name", null, 0, null };
                yield return new object[] { new FixedSchema("Test_Namespace", "TestNamespace"), "Test_Namespace", "TestNamespace", 0, null };
                yield return new object[] { new FixedSchema("Test_Documentation") { Size = 10 }, "Test_Documentation", null, 10, null };
            }
        }

        private class EnumSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new EnumSchema("Test_Name"), "Test_Name", null, new string[0], null, null };
                yield return new object[] { new EnumSchema("Test_Name", null, new string[] { "A", "C", "B" }), "Test_Name", null, new string[] { "A", "C", "B" }, null, null };
                yield return new object[] { new EnumSchema("Test_Name", null, new string[] { "int", "Type", "B" }), "Test_Name", null, new string[] { "_int", "Type", "B" }, null, null };
                yield return new object[] { new EnumSchema("Test_Namespace", "TestNamespace"), "Test_Namespace", "TestNamespace", new string[0], null, null };
                yield return new object[] { new EnumSchema("Test_Documentation") { Doc = "Test Enum Documentation" }, "Test_Documentation", null, new string[0], "Test Enum Documentation", null };
            }
        }

        private class RecordSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new RecordSchema("Test_Name"), "Test_Name", null, null, null };
                yield return new object[] { new RecordSchema("Test_Namespace", "TestNamespace"), "Test_Namespace", "TestNamespace", null, null };
                yield return new object[] { new RecordSchema("Test_Documentation") { Doc = "Test Record Documentation" }, "Test_Documentation", null, "Test Record Documentation", null };
                yield return new object[] { new RecordSchema("Test_Alias") { Aliases = new List<string>() { "Key1", "Key2" } }, "Test_Alias", null, null, new string[] { "Key1", "Key2" } };
            }
        }

        private class ErrorSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new ErrorSchema("Test_Name"), "Test_Name", null, null, null };
                yield return new object[] { new ErrorSchema("Test_Namespace", "TestNamespace"), "Test_Namespace", "TestNamespace", null, null };
                yield return new object[] { new ErrorSchema("Test_Documentation") { Doc = "Test Error Documentation" }, "Test_Documentation", null, "Test Error Documentation", null };
                yield return new object[] { new ErrorSchema("Test_Alias") { Aliases = new List<string>() { "X", "Y" } }, "Test_Alias", null, null, new string[] { "X", "Y" } };
            }
        }

        private class RecordFieldSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new IntSchema()) { Doc = "Field Doc" } }, "Field1", "Field Doc", null };
                yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field2", new StringSchema()) { Aliases = new string[] { "OldField" } } }, "Field2", null, new string[] { "OldField" } };
            }
        }

        private static string GetSummaryText(XmlDocument doc, string typeFullName, string propertyName = null)
        {
            var key = propertyName == null ? $"T:{typeFullName}" : $"P:{typeFullName}.{propertyName}";
            var summaryText = doc.SelectSingleNode($"//doc/members/member[@name='{key}']/summary/text()");
            return summaryText?.InnerText.Trim(' ', '\t', '\r', '\f', '\n');
        }

        private static string[] GetAliasList(XmlDocument doc, string typeFullName, string propertyName = null)
        {
            var key = propertyName == null ? $"T:{typeFullName}" : $"P:{typeFullName}.{propertyName}";
            var aliasList = doc.SelectNodes($"//doc/members/member[@name='{key}']/summary/list/listheader/term[text()='Aliases']/../../item/term/text()");
            if (aliasList.Count == 0)
                return null;
            return aliasList.OfType<XmlNode>().Select(r => r.Value).ToArray();
        }
    }
}
