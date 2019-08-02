using Avro;
using Avro.Schemas;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Test.Schemas
{
    [TestFixture(TypeArgs = new Type[] { typeof(IntSchema) })]
    [TestFixture(TypeArgs = new Type[] { typeof(RecordSchema.Field) })]
    public class SchemaTagTest<T> where T : AvroObject, new()
    {
        private readonly AvroObject _avroObject;

        public SchemaTagTest()
        {
            _avroObject = new T();
        }
        [TestCase("SomeTag", int.MaxValue, "Some Text")]
        public void InstanceSingleTag(string tag, object value, object newValue)
        {
            Assert.DoesNotThrow(() => _avroObject.AddTag(tag, value));
            Assert.AreEqual(value, _avroObject.Tags[tag]);
            Assert.DoesNotThrow(() => _avroObject.SetTag(tag, newValue));
            Assert.AreEqual(newValue, _avroObject.Tags[tag]);
            Assert.DoesNotThrow(() => _avroObject.RemoveTag(tag));
            Assert.IsFalse(_avroObject.Tags.ContainsKey(tag));
        }

        [TestCase(12)]
        public void InstanceMultipleTags(int count)
        {
            var tags = new int[count];
            for (int i = 0; i < count; i++)
                tags[i] = i;

            Assert.DoesNotThrow(() => _avroObject.AddTags(tags.ToDictionary(k => $"Key{k}", v => (object)$"Value{v}")));
            Assert.AreEqual(count, _avroObject.Tags.Count);
        }

        [TestCase("record")]
        public void InstanceReservedTags(string tag)
        {
            Assert.Throws(typeof(InvalidOperationException), () => _avroObject.AddTag(tag, null));
        }
    }

    [TestFixture]
    public class UnionTagTest
    {
        [TestCase]
        public void TestUnion()
        {
            var schema = new UnionSchema() as AvroObject;
            Assert.Throws(typeof(NotSupportedException), () => { schema.AddTag("tagKey1", null); });
            Assert.Throws(typeof(NotSupportedException), () => { schema.AddTags(new List<KeyValuePair<string, object>>());});
            Assert.Throws(typeof(NotSupportedException), () => { schema.RemoveTag("tagKey1"); });
            Assert.Throws(typeof(NotSupportedException), () => { schema.SetTag("tagKey1", null); });


            Assert.IsEmpty(schema.Tags.Values);
        }
    }
}
