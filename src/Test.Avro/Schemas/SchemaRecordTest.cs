using Avro;
using Avro.Schema;
using NUnit.Framework;
using System.Collections;

namespace Test.Avro.Schema
{
    [TestFixture]
    class SchemaRecordTest
    {
        [TestCase]
        public void AddEmptyField()
        {
            var record = new RecordSchema("TestRecord");
            Assert.Throws(typeof(AvroParseException), () => record.Add(new RecordFieldSchema()));
        }

        [TestCase]
        public void AddDuplicateField()
        {
            var record = new RecordSchema("TestRecord");
            var field = new RecordFieldSchema("TestField");
            record.Add(field);
            Assert.Throws(typeof(AvroParseException), () => record.Add(field));
        }

        [TestCase]
        public void RemoveField()
        {
            var record = new RecordSchema("TestRecord");
            var field = new RecordFieldSchema("TestField");
            record.Add(field);
            Assert.IsFalse(record.Remove("X"));
            Assert.IsTrue(record.Remove(field.Name));
            Assert.IsEmpty(record);
        }
    }
}
