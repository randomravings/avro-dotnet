using Avro;
using Avro.Schema;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Test.Avro.Schema
{
    [TestFixture(TypeArgs = new Type[] { typeof(IntSchema) })]
    [TestFixture(TypeArgs = new Type[] { typeof(FieldSchema) })]

    public class SchemaTagTest<T> where T : AvroSchema, new()
    {
        private readonly AvroSchema _avroSchema;

        public SchemaTagTest()
        {
            _avroSchema = new T();
        }
        [TestCase("SomeTag", int.MaxValue)]
        public void InstanceSingleTag(string tag, object value)
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
            Assert.Throws(typeof(InvalidOperationException), () => _avroSchema.AddTag(tag, string.Empty));
        }
    }

    [TestFixture]
    public class UnionTagTest
    {
        [TestCase]
        public void TestUnion()
        {
            var schema = new UnionSchema() as AvroSchema;
            Assert.Throws(typeof(NotSupportedException), () => { schema.AddTag("tagKey1", string.Empty); });
            Assert.Throws(typeof(NotSupportedException), () => { schema.AddTags(new List<KeyValuePair<string, object>>()); });
            Assert.Throws(typeof(NotSupportedException), () => { schema.RemoveTag("tagKey1"); });


            Assert.IsEmpty(schema.Tags.Values);
        }
    }
}
