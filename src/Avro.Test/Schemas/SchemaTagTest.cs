using Avro;
using Avro.Schema;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Test.Schema
{
    [TestFixture(TypeArgs = new Type[] { typeof(IntSchema) })]
    [TestFixture(TypeArgs = new Type[] { typeof(RecordSchema.Field) })]
    public class SchemaTagTest<T> where T : AvroSchema, new()
    {
        private readonly AvroSchema _avroSchema;

        public SchemaTagTest()
        {
            _avroSchema = new T();
        }
        [TestCase("SomeTag", int.MaxValue, "Some Text")]
        public void InstanceSingleTag(string tag, object value, object newValue)
        {
            Assert.DoesNotThrow(() => _avroSchema.AddTag(tag, value));
            Assert.AreEqual(value, _avroSchema.Tags[tag]);
            Assert.DoesNotThrow(() => _avroSchema.RemoveTag(tag));
            Assert.IsFalse(_avroSchema.Tags.ContainsKey(tag));
        }

        [TestCase(12)]
        public void InstanceMultipleTags(int count)
        {
            var tags = new int[count];
            for (int i = 0; i < count; i++)
                tags[i] = i;

            Assert.DoesNotThrow(() => _avroSchema.AddTags(tags.ToDictionary(k => $"Key{k}", v => (object)$"Value{v}")));
            Assert.AreEqual(count, _avroSchema.Tags.Count);
        }

        [TestCase("record")]
        public void InstanceReservedTags(string tag)
        {
            Assert.Throws(typeof(InvalidOperationException), () => _avroSchema.AddTag(tag, null));
        }
    }

    [TestFixture]
    public class UnionTagTest
    {
        [TestCase]
        public void TestUnion()
        {
            var schema = new UnionSchema() as AvroSchema;
            Assert.Throws(typeof(NotSupportedException), () => { schema.AddTag("tagKey1", null); });
            Assert.Throws(typeof(NotSupportedException), () => { schema.AddTags(new List<KeyValuePair<string, object>>());});
            Assert.Throws(typeof(NotSupportedException), () => { schema.RemoveTag("tagKey1"); });


            Assert.IsEmpty(schema.Tags.Values);
        }
    }
}
