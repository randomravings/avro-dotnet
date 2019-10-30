using Avro;
using Avro.Schema;
using NUnit.Framework;
using System.Collections;

namespace Test.Avro.Schema
{
    [TestFixture]
    class SchemaEqualsTest
    {
        [Test, TestCaseSource(typeof(EqualitySchemas))]
        public void SchemaEquality(AvroSchema schemaA, AvroSchema schemaB)
        {
            Assert.AreEqual(schemaA, schemaA);
            Assert.AreNotEqual(schemaA, schemaB);
        }

        class EqualitySchemas : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new IntSchema(), new LongSchema() };
                yield return new object[] { new FixedSchema("FixedA", string.Empty, 14), new FixedSchema("FixedB", string.Empty, 14) };
                yield return new object[] { new FixedSchema("FixedA", string.Empty, 14), new FixedSchema("FixedA", string.Empty, 16) };
                yield return new object[] { new DecimalSchema(12, 5), new DecimalSchema(15, 5) };
                yield return new object[] { new DecimalSchema(15, 5), new DecimalSchema(15, 7) };
                yield return new object[] { new ArraySchema(new IntSchema()), new ArraySchema(new LongSchema()) };
                yield return new object[] { new MapSchema(new IntSchema()), new MapSchema(new LongSchema()) };
            }
        }
    }
}
