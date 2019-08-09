using Avro.Generic;
using Avro.IO;
using Avro.Schemas;
using Avro.Specific;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Avro.Test.Generic
{
    [TestFixture()]
    public class GenericReadWriteAdvTest
    {
        private EnumSchema _enumSchema;
        private FixedSchema _fixedSchema;
        private RecordSchema _subRecordSchema;
        private RecordSchema _recordSchema;
        private RecordSchema _testRecordWithDefault;
        private RecordSchema _testRecordWithoutDefault;
        private RecordSchema _testRecordWithExtraField;

        [SetUp]
        public void SetUp()
        {
            _enumSchema = AvroReader.ReadSchema(@"{""name"":""Avro.Test.Specific.TestEnum"",""type"":""enum"",""symbols"":[""A"",""B"",""C""]}") as EnumSchema;
            _fixedSchema = AvroReader.ReadSchema(@"{""name"":""Avro.Test.Specific.TestFixed"",""type"":""fixed"",""size"":40}") as FixedSchema;
            _subRecordSchema = AvroReader.ReadSchema(@"{""name"":""Avro.Test.Specific.TestSubRecord"",""type"":""record"",""fields"":[{""name"":""FieldD"",""type"":""boolean""}]}") as RecordSchema;
            _recordSchema = AvroReader.ReadSchema(@"{""name"":""Avro.Test.Specific.TestRecord"",""type"":""record"",""fields"":[{""name"":""FieldA"",""type"":""int""},{""name"":""FieldB"",""type"":""string""},{""name"":""FieldC"",""type"":{""name"":""Avro.Test.Specific.TestSubRecord"",""type"":""record"",""fields"":[{""name"":""FieldD"",""type"":""boolean""}]}},{""name"":""FieldX"",""type"":{""name"":""Avro.Test.Specific.TestEnum"",""type"":""enum"",""symbols"":[""A"",""B"",""C""]}},{""name"":""TestFixed"",""type"":{""name"":""Avro.Test.Specific.TestFixed"",""type"":""fixed"",""size"":40}}]}") as RecordSchema;

            _testRecordWithDefault = AvroReader.ReadSchema(@"{""name"":""Avro.Test.Specific.TestRecordWithDefault"",""type"":""record"",""fields"":[{""name"":""ID"",""type"":""int"",""default"":-1},{""name"":""Name"",""type"":""string""}]}") as RecordSchema;
            _testRecordWithoutDefault = AvroReader.ReadSchema(@"{""name"":""Avro.Test.Specific.TestRecordWithDefault"",""type"":""record"",""fields"":[{""name"":""ID"",""type"":""int""},{""name"":""Name"",""type"":""string""}]}") as RecordSchema;
            _testRecordWithExtraField = AvroReader.ReadSchema(@"{""name"":""Avro.Test.Specific.TestRecordWithDefault"",""type"":""record"",""fields"":[{""name"":""Name"",""type"":""string""},{""name"":""Desc"",""type"":""string""}]}") as RecordSchema;
        }

        [TestCase]
        public void EnumTest()
        {
            var writer = new GenericWriter<GenericEnum>(_enumSchema);
            var reader = new GenericReader<GenericEnum>(_enumSchema, _enumSchema);

            var expectedValue = new GenericEnum(_enumSchema, "B");
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = reader.Read(decoder);
                Assert.AreEqual(expectedValue, actualValue);

                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedPosition, stream.Position);
            }
        }

        [TestCase]
        public void RecordTest()
        {
            var recordInstance = new GenericRecord(_recordSchema);
            var subRecordInstance = new GenericRecord(_subRecordSchema);
            var fixedInstance = new GenericFixed(_fixedSchema);
            var enumInstance = new GenericEnum(_enumSchema, "B");

            subRecordInstance["FieldD"] = false;

            recordInstance["FieldA"] = 123;
            recordInstance["FieldB"] = "Test";
            recordInstance["FieldC"] = subRecordInstance;
            recordInstance["FieldX"] = enumInstance;
            recordInstance["TestFixed"] = fixedInstance;

            var writer = new GenericWriter<GenericRecord>(_recordSchema);
            var reader = new GenericReader<GenericRecord>(_recordSchema, _recordSchema);

            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, recordInstance);
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = reader.Read(decoder);
                for (int i = 0; i < recordInstance.Schema.Count; i++)
                    Assert.AreEqual(recordInstance[i], actualValue[i]);

                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedPosition, stream.Position);
            }
        }

        [TestCase]
        public void RecordTestWithOverlap()
        {
            var expectedValue = new GenericRecord(_testRecordWithDefault);
            expectedValue["Name"] = "Test";

            var writeValue = new GenericRecord(_testRecordWithExtraField);
            writeValue["Name"] = "Test";
            writeValue["Desc"] = "Description";

            var writer = new GenericWriter<GenericRecord>(writeValue.Schema);
            var reader = new GenericReader<GenericRecord>(expectedValue.Schema, writeValue.Schema);

            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, writeValue);
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = reader.Read(decoder);
                for (int i = 0; i < _testRecordWithDefault.Count; i++)
                    Assert.AreEqual(expectedValue[i], actualValue[i]);

                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedPosition, stream.Position);
            }
        }

        [TestCase]
        public void RecordTestWithoutOverlap()
        {
            var expectedValue = new GenericRecord(_testRecordWithoutDefault);
            expectedValue["ID"] = 123;
            expectedValue["Name"] = "Test";

            var writeValue = new GenericRecord(_testRecordWithExtraField);
            writeValue["Name"] = "Test";
            writeValue["Desc"] = "Description";

            var writer = new GenericWriter<GenericRecord>(writeValue.Schema);
            Assert.Throws<AvroException>(() => new GenericReader<GenericRecord>(expectedValue.Schema, writeValue.Schema));
        }

        [TestCase]
        public void FixedTest()
        {
            var expectedValue = new GenericFixed(_fixedSchema);
            expectedValue.Value[1] = 1;

            for (int i = 2; i < expectedValue.Size; i++)
                expectedValue.Value[i] = (byte)((expectedValue.Value[i - 2] + expectedValue.Value[i - 1]) % byte.MaxValue);

            var writer = new GenericWriter<GenericFixed>(expectedValue.Schema);
            var reader = new GenericReader<GenericFixed>(expectedValue.Schema, expectedValue.Schema);

            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = reader.Read(decoder);
                Assert.AreEqual(expectedValue.Value, actualValue.Value);

                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedPosition, stream.Position);
            }
        }

        [TestCase]
        public void NullableValueTest()
        {
            var writerSchema = new UnionSchema(new FloatSchema(), new NullSchema());
            var readerSchema = new UnionSchema(new NullSchema(), new FloatSchema());
            var writer = new GenericWriter<float?>(writerSchema);
            var reader = new GenericReader<float?>(readerSchema, writerSchema);

            var expectedValueNotNull = new float?(56.45F);
            var expectedValueNull = new float?();
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValueNotNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNotNull, reader.Read(decoder));

                var expectedNoNullPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedNoNullPosition, stream.Position);

                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedValueNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNull, reader.Read(decoder));

                var expectedNullPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedNullPosition, stream.Position);
            }
        }

        [TestCase]
        public void NullableReferenceTest()
        {
            var writerSchema = new UnionSchema(new StringSchema(), new NullSchema());
            var readerSchema = new UnionSchema(new NullSchema(), new StringSchema());
            var writer = new GenericWriter<string>(writerSchema);
            var reader = new GenericReader<string>(readerSchema, writerSchema);

            var expectedValueNotNull = "Some String";
            var expectedValueNull = null as string;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValueNotNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNotNull, reader.Read(decoder));

                var expectedNoNullPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedNoNullPosition, stream.Position);

                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedValueNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNull, reader.Read(decoder));

                var expectedNullPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedNullPosition, stream.Position);
            }
        }

        [TestCase]
        public void NullableNullWriterTest()
        {
            var writerSchema = new NullSchema();
            var readerSchema = new UnionSchema(new NullSchema(), new StringSchema());
            var writer = new GenericWriter<string>(writerSchema);
            var reader = new GenericReader<string>(readerSchema, writerSchema);

            var expectedValueNotNull = "Some String";
            var expectedValueNull = null as string;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValueNotNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNull, reader.Read(decoder));

                var expectedNoNullPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedNoNullPosition, stream.Position);

                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedValueNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNull, reader.Read(decoder));

                var expectedNullPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedNullPosition, stream.Position);
            }
        }

        [TestCase]
        public void NullableNotNullWriterTest()
        {
            var writerSchema = new IntSchema();
            var readerSchema = new UnionSchema(new NullSchema(), new IntSchema());
            var writer = new GenericWriter<int>(writerSchema);
            var reader = new GenericReader<int?>(readerSchema, writerSchema);

            var expectedValueNotNull = 123;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValueNotNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNotNull, reader.Read(decoder));

                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedPosition, stream.Position);
            }
        }

        [TestCase]
        public void NullableArbitraryUnionTest()
        {
            var writerSchema = new UnionSchema(new StringSchema(), new DoubleSchema(), new UuidSchema(), new NullSchema());
            var readerSchema = new UnionSchema(new StringSchema(), new NullSchema());
            var writer = new GenericWriter<object>(writerSchema);
            var reader = new GenericReader<string>(readerSchema, writerSchema);

            var expectedValueNotNull = "Some String";
            var expectedValueNull = null as string;
            var expectedIndexOutOfRange = 2.34D;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValueNotNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNotNull, reader.Read(decoder));

                var expectedNoNullPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedNoNullPosition, stream.Position);

                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedValueNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNull, reader.Read(decoder));

                var expectedNullPosition = stream.Position;
                writer.Write(encoder, expectedValueNotNull);
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedNullPosition, stream.Position);

                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedIndexOutOfRange);

                var expectedExceptionPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedExceptionPosition, stream.Position);

                writer.Write(encoder, expectedIndexOutOfRange);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.Throws<IndexOutOfRangeException>(() => reader.Read(decoder));
            }
        }


        [TestCase]
        public void NonUnionReferenceToUnionTest()
        {
            var writerSchema = new StringSchema();
            var readerSchema = new UnionSchema(new FloatSchema(), new StringSchema(), new BytesSchema());
            var writer = new GenericWriter<string>(writerSchema);
            var reader = new GenericReader<object>(readerSchema, writerSchema);

            var expectedValue = "Test String";
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValue, reader.Read(decoder));

                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedPosition, stream.Position);
            }
        }

        [TestCase]
        public void NonUnionValueToUnionTest()
        {
            var writerSchema = new FloatSchema();
            var readerSchema = new UnionSchema(new FloatSchema(), new StringSchema(), new BytesSchema());
            var writer = new GenericWriter<float>(writerSchema);
            var reader = new GenericReader<object>(readerSchema, writerSchema);

            var expectedValue = 123.456F;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValue, reader.Read(decoder));

                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedPosition, stream.Position);
            }
        }

        [TestCase]
        public void UnionToNonUnionTest()
        {
            var writerSchema = new UnionSchema(new BooleanSchema(), new LongSchema(), new DoubleSchema(), new StringSchema());
            var readerSchema = new LongSchema();
            var writer = new GenericWriter<object>(writerSchema);
            var reader = new GenericReader<long>(readerSchema, writerSchema);

            var expectedValue = 56723234L;
            var expectedIndexOutOfRange = "Index out of range ...";
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValue, reader.Read(decoder));

                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedPosition, stream.Position);

                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedIndexOutOfRange);

                var expectedExceptionPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedExceptionPosition, stream.Position);

                writer.Write(encoder, expectedIndexOutOfRange);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.Throws<IndexOutOfRangeException>(() => reader.Read(decoder));
            }
        }

        [TestCase]
        public void UnionToUnionMixedTest()
        {
            var writerSchema = new UnionSchema(new BooleanSchema(), new LongSchema(), new DoubleSchema(), new StringSchema());
            var readerSchema = new UnionSchema(new NullSchema(), new IntSchema(), new FloatSchema(), new BytesSchema());
            var writer = new GenericWriter<object>(writerSchema);
            var reader = new GenericReader<object>(readerSchema, writerSchema);

            var expectedValue = 56723234L;
            var expectedIndexOutOfRange = true;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValue, reader.Read(decoder));

                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedPosition, stream.Position);

                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedIndexOutOfRange);

                var expectedExceptionPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedExceptionPosition, stream.Position);

                writer.Write(encoder, expectedIndexOutOfRange);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.Throws<IndexOutOfRangeException>(() => reader.Read(decoder));
            }
        }

        [TestCase]
        public void ArrayTest()
        {
            var writerSchema = new ArraySchema(new IntSchema());
            var readerSchema = new ArraySchema(new LongSchema());
            var writer = new GenericWriter<IList<object>>(writerSchema);
            var reader = new GenericReader<IList<object>>(readerSchema, writerSchema);

            var expectedArray = new List<object> { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };

            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedArray);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedArray, reader.Read(decoder));

                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedPosition, stream.Position);

            }
        }

        [TestCase]
        public void MapTest()
        {
            var writerSchema = new MapSchema(new FloatSchema());
            var readerSchema = new MapSchema(new DoubleSchema());
            var writer = new GenericWriter<IDictionary<string, object>>(writerSchema);
            var reader = new GenericReader<IDictionary<string, object>>(readerSchema, writerSchema);

            var expectedMap = new Dictionary<string, object> { { "Key1", 1.1F }, { "Key2", 2.2F }, { "Key3", 3.3F }, { "Key4", 4.4F }, { "Key5", 5.5F } };

            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedMap);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedMap, reader.Read(decoder));

                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                reader.Skip(decoder);
                Assert.AreEqual(expectedPosition, stream.Position);

            }
        }

        [TestCase]
        public void UnresolvedSchema()
        {
            var writerSchema = new IntSchema();
            var readerSchema = new BytesSchema();

            Assert.Throws<AvroException>(() => new GenericWriter<string>(writerSchema));
            Assert.Throws<AvroException>(() => new GenericReader<IList<object>>(readerSchema, writerSchema));
        }
    }
}
