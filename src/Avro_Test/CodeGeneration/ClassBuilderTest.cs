using Avro.CodeDom;
using Avro.Schemas;
using NUnit.Framework;
using System;
using System.Collections;
using System.Linq;

namespace CodeGeneration
{
    public class ClassBuilderTest
    {
        [Test, TestCaseSource(typeof(FixedSource))]
        public void FixedCode(FixedSchema fixedSchema, string expectedName, string expectedNamespace, int expectedSize)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), fixedSchema, out var xmlDocument);

            var type = assembly.ExportedTypes.FirstOrDefault(r => r.Name == expectedName);
            Assert.NotNull(type);
            Assert.IsTrue(type.IsClass);
            Assert.AreEqual(expectedNamespace, type.Namespace);

            var size = Convert.ToInt32(type.GetField("_SIZE").GetValue(null));
            Assert.AreEqual(expectedSize, size);
        }

        [Test, TestCaseSource(typeof(EnumSource))]
        public void EnumCode(EnumSchema enumSchema, string expectedName, string expectedNamespace, string expectedDoc)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), enumSchema, out var xmlDocument);

            var type = assembly.ExportedTypes.FirstOrDefault(r => r.Name == expectedName);
            Assert.NotNull(type);
            Assert.IsTrue(type.IsEnum);
            Assert.AreEqual(expectedNamespace, type.Namespace);

            var doc = xmlDocument.SelectSingleNode($"//doc/members/member[@name='T:{expectedName}']/summary");
            Assert.AreEqual(expectedDoc, doc?.InnerText.Trim(' ', '\t', '\r', '\f', '\n') ?? null);
        }

        [Test, TestCaseSource(typeof(RecordSource))]
        public void RecordCode(RecordSchema recordSchema, string expectedName, string expectedNamespace, string expectedDoc)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), recordSchema, out var xmlDocument);

            var type = assembly.ExportedTypes.FirstOrDefault(r => r.Name == expectedName);
            Assert.NotNull(type);
            Assert.IsTrue(type.IsClass);
            Assert.AreEqual(expectedNamespace, type.Namespace);

            var doc = xmlDocument.SelectSingleNode($"//doc/members/member[@name='T:{expectedName}']/summary");
            Assert.AreEqual(expectedDoc, doc?.InnerText.Trim(' ', '\t', '\r', '\f', '\n') ?? null);
        }

        [Test, TestCaseSource(typeof(ErrorSource))]
        public void ErrorCode(ErrorSchema errorSchema, string expectedName, string expectedNamespace, string expectedDoc)
        {
            var assembly = CodeGen.Compile(Guid.NewGuid().ToString(), errorSchema, out var xmlDocument);

            var type = assembly.ExportedTypes.FirstOrDefault(r => r.Name == expectedName);
            Assert.NotNull(type);
            Assert.IsTrue(type.IsClass);
            Assert.AreEqual(expectedNamespace, type.Namespace);

            var doc = xmlDocument.SelectSingleNode($"//doc/members/member[@name='T:{expectedName}']/summary");
            Assert.AreEqual(expectedDoc, doc?.InnerText.Trim(' ', '\t', '\r', '\f', '\n') ?? null);
        }

        private class FixedSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new FixedSchema("Test_Name"), "Test_Name", null, 0 };
                yield return new object[] { new FixedSchema("Test_Namespace", "TestNamespace"), "Test_Namespace", "TestNamespace", 0 };
                yield return new object[] { new FixedSchema("Test_Documentation") { Size = 10 }, "Test_Documentation", null, 10 };
            }
        }

        private class EnumSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new EnumSchema("Test_Name"), "Test_Name", null, null };
                yield return new object[] { new EnumSchema("Test_Namespace", "TestNamespace"), "Test_Namespace", "TestNamespace", null };
                yield return new object[] { new EnumSchema("Test_Documentation") { Doc = "Test Enum Documentation" }, "Test_Documentation", null, "Test Enum Documentation" };
            }
        }

        private class RecordSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new RecordSchema("Test_Name"), "Test_Name", null, null };
                yield return new object[] { new RecordSchema("Test_Namespace", "TestNamespace"), "Test_Namespace", "TestNamespace", null };
                yield return new object[] { new RecordSchema("Test_Documentation") { Doc = "Test Record Documentation" }, "Test_Documentation", null, "Test Record Documentation" };
            }
        }

        private class ErrorSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new ErrorSchema("Test_Name"), "Test_Name", null, null };
                yield return new object[] { new ErrorSchema("Test_Namespace", "TestNamespace"), "Test_Namespace", "TestNamespace", null };
                yield return new object[] { new ErrorSchema("Test_Documentation") { Doc = "Test Record Documentation" }, "Test_Documentation", null, "Test Record Documentation" };
            }
        }
    }
}
