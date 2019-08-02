using Avro;
using Avro.Schemas;
using NUnit.Framework;
using System.Collections;

namespace Avro.Test.Schemas
{
    [TestFixture()]
    class SchemaRecordTest
    {
        [TestCase]
        public void AddEmptyField()
        {
            var record = new RecordSchema("TestRecord");
            Assert.Throws(typeof(AvroParseException), () => record.Add(null));
            Assert.Throws(typeof(AvroParseException), () => record.Add(new RecordSchema.Field()));
        }

        [TestCase]
        public void AddDuplicateField()
        {
            var record = new RecordSchema("TestRecord");
            var field = new RecordSchema.Field("TestField");
            record.Add(field);
            Assert.Throws(typeof(AvroParseException), () => record.Add(field));
        }

        [TestCase]
        public void RemoveField()
        {
            var record = new RecordSchema("TestRecord");
            var field = new RecordSchema.Field("TestField");
            record.Add(field);
            Assert.IsFalse(record.Remove("X"));
            Assert.IsTrue(record.Remove(field.Name));
            Assert.IsEmpty(record);
        }
    }
}
