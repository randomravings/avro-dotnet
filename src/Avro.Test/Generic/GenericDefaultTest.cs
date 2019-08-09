using Avro.Generic;
using Avro.Schemas;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Avro.Test.Generic
{
    [TestFixture]
    public class GenericDefaultTest
    {
        [Test, TestCaseSource(typeof(RecordFieldDefaultSource))]
        public void TestGenericDefault(RecordSchema schema, string fieldName, object expectedValue)
        {
            var record = new GenericRecord(schema);
            Assert.AreEqual(expectedValue, record[fieldName]);
        }

        [TestCase]
        public void TestGenericRecordDefault()
        {
            var schema =
                new RecordSchema("X") {
                    new RecordSchema.Field(
                        "Field1",
                        new RecordSchema(
                            "Y",
                            null,
                            new RecordSchema.Field[] {
                                new RecordSchema.Field(
                                    "f1",
                                    new IntSchema()
                                ),
                                new RecordSchema.Field(
                                    "f2",
                                    new RecordSchema(
                                        "Z",
                                        null,
                                        new RecordSchema.Field[] {
                                            new RecordSchema.Field(
                                                "f3",
                                                new StringSchema()
                                            ),
                                            new RecordSchema.Field(
                                                "f4",
                                                new FloatSchema()
                                            )
                                        }
                                    )
                                )
                            }
                        )
                    ) { Default = JToken.Parse(@"{""f1"":123,""f2"":{""f3"":""ABC"",""f4"":0.0012}}") }
                };

            var record = new GenericRecord(schema);

            var record01 = record[0] as GenericRecord;
            Assert.IsNotNull(record01);
            Assert.AreEqual(123, record01[0]);
            var record02 = record01[1] as GenericRecord;
            Assert.IsNotNull(record02);
            Assert.AreEqual("ABC", record02[0]);
            Assert.AreEqual(0.0012F, record02[1]);
        }
    }

    class RecordFieldDefaultSource : IEnumerable
    {
        public IEnumerator GetEnumerator()
        {
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new NullSchema()) { Default = null } }, "Field1", null };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new BooleanSchema()) { Default = true } }, "Field1", true };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new IntSchema()) { Default = 123 } }, "Field1", 123 };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new LongSchema()) { Default = 987654321L } }, "Field1", 987654321L };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new FloatSchema()) { Default = 98765.4321F } }, "Field1", 98765.4321F };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new DoubleSchema()) { Default = 98765.4321D } }, "Field1", 98765.4321D };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new BytesSchema()) { Default = @"\u0000\u0001\u0010\u00AB\u00FF" } }, "Field1", new byte[] { 0x00, 0x01, 0x10, 0xAB, 0xFF } };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new StringSchema()) { Default = @"""Hello World!""" } }, "Field1", "Hello World!" };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new ArraySchema(new IntSchema())) { Default = JToken.Parse("[1, 2, 4]") } }, "Field1", new List<int>() { 1, 2, 4 } };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new MapSchema(new IntSchema())) { Default = JToken.Parse(@"{""key1"":1, ""key2"":2, ""key3"":4}") } }, "Field1", new Dictionary<string, int>() { { "key1", 1 }, { "key2", 2 }, { "key3", 4 } } };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new UuidSchema()) { Default = @"""61FEDC68-47CA-4727-BDFF-685A4E3EC846""" } }, "Field1", new Guid("61FEDC68-47CA-4727-BDFF-685A4E3EC846") };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new UnionSchema(new NullSchema(), new IntSchema())) { Default = JValue.CreateNull() } }, "Field1", null };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new FixedSchema("Y", null, 3)) { Default = @"\u0001\u0002\u0003" } }, "Field1", new byte[] { 0x01, 0x02, 0x03 } };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new EnumSchema("Y", null, new string[] { "A", "B", "C" })) { Default = @"""B""" } }, "Field1", "B" };
            yield return new object[] { new RecordSchema("X") { new RecordSchema.Field("Field1", new LogicalSchema(new IntSchema(), "ls")) { Default = 42 } }, "Field1", 42 };
        }
    }

    class UnknownSchema : Schema { }
}
