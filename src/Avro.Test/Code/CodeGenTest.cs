using Avro;
using Avro.Code;
using Avro.Schemas;
using NUnit.Framework;
using System.Collections;
using System.Linq;

namespace Avro.Test.Code
{
    public class CodeGenTest
    {
        [TestCase]
        public void TestCompileError()
        {
            string badCode = "public class RandomClass{ public MissingTypeProperty { get; set;} }";
            Assert.Throws(
                typeof(CodeGenException),
                () => CodeGen.Compile("RandomAssembly", badCode, out _)
            );
        }

        [TestCase]
        public void TestAddSchema()
        {
            var codeGen = new CodeGen();

            var intSchema = new IntSchema();
            codeGen.AddSchema(intSchema);
            Assert.AreEqual(0, codeGen.Count);

            var enumSchema = new EnumSchema("TestEnum", "Test.Namespace", new string[] { "A", "B", "C" });
            codeGen.AddSchema(enumSchema);
            Assert.AreEqual(1, codeGen.Count);

            var fixedSchema = new FixedSchema("TestFixed", "Test.Namespace", 12);
            codeGen.AddSchema(fixedSchema);
            Assert.AreEqual(2, codeGen.Count);

            var recordSchema = new RecordSchema("TestRecord", "Test.Namespace", new RecordSchema.Field[] { new RecordSchema.Field("A", new IntSchema()) });
            codeGen.AddSchema(recordSchema);
            Assert.AreEqual(3, codeGen.Count);

            var errorSchema = new RecordSchema("TestError", "Test.Namespace", new RecordSchema.Field[] { new RecordSchema.Field("B", enumSchema) });
            codeGen.AddSchema(errorSchema);
            Assert.AreEqual(4, codeGen.Count);

            var unionSchema = new UnionSchema(fixedSchema, new BytesSchema(), new EnumSchema("TestEnum2", "Test.Namespace", new string[] { "D", "E", "F" }));
            codeGen.AddSchema(unionSchema);
            Assert.AreEqual(5, codeGen.Count);
        }

        [TestCase]
        public void TestAddProtocol()
        {
            var codeGen = new CodeGen();

            var recordSchema = new RecordSchema("TestRecord", null, new RecordSchema.Field[] { new RecordSchema.Field("A", new IntSchema()) });
            var protocol = new Protocol("TestProtocol", "Test.Namespace");
            protocol.AddType(recordSchema);

            codeGen.AddProtocol(protocol);

            Assert.AreEqual(2, codeGen.Count);
            Assert.IsNotNull(codeGen["Test.Namespace.TestProtocol"]);
            Assert.IsNotNull(codeGen["Test.Namespace.TestRecord"]);
        }

        [TestCase]
        public void TestCollectionImplementation()
        {
            var codeGen = new CodeGen();

            var enumSchema = new EnumSchema("TestEnum", "Test.Namespace", new string[] { "A", "B", "C" });
            codeGen.AddSchema(enumSchema);

            Assert.AreEqual(1, codeGen.Count);
            Assert.AreEqual(1, codeGen.Keys.Count());
            Assert.AreEqual(1, codeGen.Values.Count());
            Assert.IsTrue(codeGen.ContainsKey("Test.Namespace.TestEnum"));
            Assert.IsNotNull(codeGen["Test.Namespace.TestEnum"]);

            foreach (var item in codeGen)
            {
                Assert.IsNotNull(item.Key);
                Assert.IsNotNull(item.Value);
            }

            foreach (var item in (codeGen as IEnumerable))
                Assert.IsNotNull(item);

            Assert.IsTrue(codeGen.TryGetValue("Test.Namespace.TestEnum", out var notNullValue));
            Assert.NotNull(notNullValue);

            Assert.IsFalse(codeGen.TryGetValue("Test.Namespace.XX", out var nullValue));
            Assert.Null(nullValue);
        }

        [Test, TestCaseSource(typeof(SchemaSource))]
        public void TestGetSystemType(Schema schema, string expectedTypeString)
        {
            var actualTypeString = CodeGen.GetSystemType(schema);
            Assert.AreEqual(expectedTypeString, actualTypeString);
        }

        class SchemaSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new NullSchema(), "object" };

                yield return new object[] { new BooleanSchema(), "bool" };
                yield return new object[] { new IntSchema(), "int" };
                yield return new object[] { new LongSchema(), "long" };
                yield return new object[] { new FloatSchema(), "float" };
                yield return new object[] { new DoubleSchema(), "double" };
                yield return new object[] { new StringSchema(), "string" };
                yield return new object[] { new BytesSchema(), "byte[]" };

                yield return new object[] { new UnionSchema() { new BooleanSchema(), new NullSchema() }, "bool?" };
                yield return new object[] { new UnionSchema() { new IntSchema(), new NullSchema() }, "int?" };
                yield return new object[] { new UnionSchema() { new LongSchema(), new NullSchema() }, "long?" };
                yield return new object[] { new UnionSchema() { new FloatSchema(), new NullSchema() }, "float?" };
                yield return new object[] { new UnionSchema() { new DoubleSchema(), new NullSchema() }, "double?" };
                yield return new object[] { new UnionSchema() { new StringSchema(), new NullSchema() }, "string" };
                yield return new object[] { new UnionSchema() { new BytesSchema(), new NullSchema() }, "byte[]" };

                yield return new object[] { new UnionSchema() { new IntSchema() }, "int" };
                yield return new object[] { new UnionSchema() { new BytesSchema(), new DoubleSchema() }, "object" };

                yield return new object[] { new ArraySchema(new BooleanSchema()), "IList<bool>" };
                yield return new object[] { new MapSchema(new FloatSchema()), "IDictionary<string, float>" };

                yield return new object[] { new FixedSchema("FixedName", "Test.Namespace", 12), "Test.Namespace.FixedName" };
                yield return new object[] { new EnumSchema("EnumName", "Test.Namespace", new string[] { "int", "V" }), "Test.Namespace.EnumName" };
                yield return new object[] { new RecordSchema("RecordName", "Test.Namespace"), "Test.Namespace.RecordName" };
                yield return new object[] { new ErrorSchema("ErrorName", "Test.Namespace"), "Test.Namespace.ErrorName" };

                yield return new object[] { new DecimalSchema(), "decimal" };
                yield return new object[] { new TimeMillisSchema(), "TimeSpan" };
                yield return new object[] { new TimeMicrosSchema(), "TimeSpan" };
                yield return new object[] { new TimeNanosSchema(), "TimeSpan" };
                yield return new object[] { new TimestampMillisSchema(), "DateTime" };
                yield return new object[] { new TimestampMicrosSchema(), "DateTime" };
                yield return new object[] { new TimestampNanosSchema(), "DateTime" };
                yield return new object[] { new DateSchema(), "DateTime" };
                yield return new object[] { new DurationSchema(), "ValueTuple<int, int, int>" };
                yield return new object[] { new UuidSchema(), "Guid" };

                yield return new object[] { new UnionSchema() { new NullSchema(), new DecimalSchema() }, "decimal?" };
                yield return new object[] { new UnionSchema() { new NullSchema(), new TimeMillisSchema() }, "TimeSpan?" };
                yield return new object[] { new UnionSchema() { new NullSchema(), new TimeMicrosSchema() }, "TimeSpan?" };
                yield return new object[] { new UnionSchema() { new NullSchema(), new TimeNanosSchema() }, "TimeSpan?" };
                yield return new object[] { new UnionSchema() { new NullSchema(), new TimestampMillisSchema() }, "DateTime?" };
                yield return new object[] { new UnionSchema() { new NullSchema(), new TimestampMicrosSchema() }, "DateTime?" };
                yield return new object[] { new UnionSchema() { new NullSchema(), new TimestampNanosSchema() }, "DateTime?" };
                yield return new object[] { new UnionSchema() { new NullSchema(), new DateSchema() }, "DateTime?" };
                yield return new object[] { new UnionSchema() { new NullSchema(), new DurationSchema() }, "ValueTuple<int, int, int>?" };
                yield return new object[] { new UnionSchema() { new NullSchema(), new UuidSchema() }, "Guid?" };

                yield return new object[] { new CodeGenTestLogicalSchema(), "byte[]" };
                yield return new object[] { new UnionSchema() { new IntSchema(), new UuidSchema() }, "object" };
                yield return new object[] { new UnionSchema() { new LongSchema(), new StringSchema(), new ArraySchema(new BooleanSchema()) }, "object" };
                yield return new object[] { new CodeGenTestSchema(), "object" };
            }
        }

        public class CodeGenTestSchema : Schema { }

        public class CodeGenTestLogicalSchema : LogicalSchema
        {
            public CodeGenTestLogicalSchema()
                : base(new BytesSchema(), "test-logicaltype") { }
        }
    }
}
