using Avro;
using Avro.Schemas;
using NUnit.Framework;
using System.Collections;

namespace Avro.Test.Schemas
{
    [TestFixture()]
    class SchemaEqualsTest
    {
        [Test, TestCaseSource(typeof(EqualitySchemas))]
        public void SchemaEquality(Schema schemaA, Schema schemaB)
        {
            Assert.AreEqual(schemaA, schemaA);
            Assert.AreNotEqual(schemaA, schemaB);
        }

        class EqualitySchemas : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new IntSchema(), new LongSchema() };
                yield return new object[] { new FixedSchema("FixedA", null, 14), new FixedSchema("FixedB", null, 14) };
                yield return new object[] { new FixedSchema("FixedA", null, 14), new FixedSchema("FixedA", null, 16) };
            }
        }
    }
}
