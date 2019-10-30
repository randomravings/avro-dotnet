using Avro;
using Avro.Schema;
using NUnit.Framework;
using System.Collections;
using System.Linq;

namespace Test.Avro.Schema
{
    [TestFixture]
    class SchemaUnionTest
    {
        [TestCase]
        public void AddInvalidItems()
        {
            var unionSchema = new UnionSchema();
            var arraySchema = new ArraySchema(new IntSchema());
            var mapSchema = new MapSchema(new IntSchema());
            var enumSchema = new EnumSchema("X.Y.Z", string.Empty, new string[] { "A", "B", "C" });

            Assert.DoesNotThrow(() => unionSchema.Add(arraySchema));
            Assert.DoesNotThrow(() => unionSchema.Add(mapSchema));
            Assert.DoesNotThrow(() => unionSchema.Add(enumSchema));

            Assert.Throws(typeof(AvroParseException), () => unionSchema.Add(unionSchema));
            Assert.Throws(typeof(AvroParseException), () => unionSchema.Add(arraySchema));
            Assert.Throws(typeof(AvroParseException), () => unionSchema.Add(mapSchema));
            Assert.Throws(typeof(AvroParseException), () => unionSchema.Add(enumSchema));
        }

        [TestCase]
        public void ClearItems()
        {
            var unionSchema = new UnionSchema
            {
                new FloatSchema(),
                new BytesSchema()
            };

            Assert.IsNotEmpty(unionSchema);

            unionSchema.Clear();

            Assert.IsEmpty(unionSchema);
        }
    }
}
