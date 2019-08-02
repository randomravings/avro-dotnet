using Avro.IO;
using Avro.Schemas;
using Avro.Specific;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Avro.Test.Specific
{
    [TestFixture()]
    public class SpecificReadWriteAdvTest
    {
        [TestCase]
        public void EnumTest()
        {
            var enumSchema = new EnumSchema(nameof(TestEnum), typeof(TestEnum).Namespace, Enum.GetNames(typeof(TestEnum)));
            var writer = new SpecificWriter<TestEnum>(enumSchema);
            var reader = new SpecificReader<TestEnum>(enumSchema, enumSchema);

            var expectedValue = TestEnum.A;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = reader.Read(decoder);
                Assert.AreEqual(expectedValue, actualValue);
            }
        }

        [TestCase]
        public void RecordTest()
        {
            var expectedValue = new TestRecord() { FieldA = 123, FieldB = "Test", FieldC = new TestSubRecord() { FieldD = false }, FieldX = TestEnum.B, TestFixed = new TestFixed() };
            var writer = new SpecificWriter<TestRecord>(expectedValue.Schema);
            var reader = new SpecificReader<TestRecord>(expectedValue.Schema, expectedValue.Schema);

            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = reader.Read(decoder);
                for (int i = 0; i < expectedValue.FieldCount; i++)
                    Assert.AreEqual(expectedValue.Get(i), actualValue.Get(i));
            }
        }

        [TestCase]
        public void FixedTest()
        {
            var expectedValue = new TestFixed();
            expectedValue.Value[1] = 1;

            for (int i = 2; i < expectedValue.FixedSize; i++)
                expectedValue.Value[i] = (byte)((expectedValue.Value[i - 2] + expectedValue.Value[i - 1]) % byte.MaxValue);

            var writer = new SpecificWriter<TestFixed>(expectedValue.Schema);
            var reader = new SpecificReader<TestFixed>(expectedValue.Schema, expectedValue.Schema);

            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = reader.Read(decoder);
                Assert.AreEqual(expectedValue.Value, actualValue.Value);
            }
        }

        [TestCase]
        public void NullableValueTest()
        {
            var writerSchema = new UnionSchema(new FloatSchema(), new NullSchema());
            var readerSchema = new UnionSchema(new NullSchema(), new FloatSchema());
            var writer = new SpecificWriter<float?>(writerSchema);
            var reader = new SpecificReader<float?>(readerSchema, writerSchema);

            var expectedValueNotNull = new float?(56.45F);
            var expectedValueNull = new float?();
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValueNotNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNotNull, reader.Read(decoder));
                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedValueNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNull, reader.Read(decoder));
            }
        }

        [TestCase]
        public void NullableReferenceTest()
        {
            var writerSchema = new UnionSchema(new StringSchema(), new NullSchema());
            var readerSchema = new UnionSchema(new NullSchema(), new StringSchema());
            var writer = new SpecificWriter<string>(writerSchema);
            var reader = new SpecificReader<string>(readerSchema, writerSchema);

            var expectedValueNotNull = "Some String";
            var expectedValueNull = null as string;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValueNotNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNotNull, reader.Read(decoder));
                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedValueNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNull, reader.Read(decoder));
            }
        }

        [TestCase]
        public void NullableNullWriterTest()
        {
            var writerSchema = new NullSchema();
            var readerSchema = new UnionSchema(new NullSchema(), new StringSchema());
            var writer = new SpecificWriter<string>(writerSchema);
            var reader = new SpecificReader<string>(readerSchema, writerSchema);

            var expectedValueNotNull = "Some String";
            var expectedValueNull = null as string;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValueNotNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNull, reader.Read(decoder));
                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedValueNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNull, reader.Read(decoder));
            }
        }

        [TestCase]
        public void NullableNotNullWriterTest()
        {
            var writerSchema = new IntSchema();
            var readerSchema = new UnionSchema(new NullSchema(), new IntSchema());
            var writer = new SpecificWriter<int>(writerSchema);
            var reader = new SpecificReader<int?>(readerSchema, writerSchema);

            var expectedValueNotNull = 123;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValueNotNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNotNull, reader.Read(decoder));
                stream.Seek(0, SeekOrigin.Begin);
            }
        }

        [TestCase]
        public void NullableArbitraryUnionTest()
        {
            var writerSchema = new UnionSchema(new StringSchema(), new DoubleSchema(), new UuidSchema(), new NullSchema());
            var readerSchema = new UnionSchema(new StringSchema(), new NullSchema());
            var writer = new SpecificWriter<object>(writerSchema);
            var reader = new SpecificReader<string>(readerSchema, writerSchema);

            var expectedValueNotNull = "Some String";
            var expectedValueNull = null as string;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValueNotNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNotNull, reader.Read(decoder));
                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedValueNull);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValueNull, reader.Read(decoder));
            }
        }


        [TestCase]
        public void NonUnionReferenceToUnionTest()
        {
            var writerSchema = new StringSchema();
            var readerSchema = new UnionSchema(new FloatSchema(), new StringSchema(), new BytesSchema());
            var writer = new SpecificWriter<string>(writerSchema);
            var reader = new SpecificReader<object>(readerSchema, writerSchema);

            var expectedValue = "Test String";
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValue, reader.Read(decoder));
            }
        }

        [TestCase]
        public void NonUnionValueToUnionTest()
        {
            var writerSchema = new FloatSchema();
            var readerSchema = new UnionSchema(new FloatSchema(), new StringSchema(), new BytesSchema());
            var writer = new SpecificWriter<float>(writerSchema);
            var reader = new SpecificReader<object>(readerSchema, writerSchema);

            var expectedValue = 123.456F;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValue, reader.Read(decoder));
            }
        }

        [TestCase]
        public void UnionToNonUnionTest()
        {
            var writerSchema = new UnionSchema(new BooleanSchema(), new LongSchema(), new DoubleSchema(), new StringSchema());
            var readerSchema = new LongSchema();
            var writer = new SpecificWriter<object>(writerSchema);
            var reader = new SpecificReader<long>(readerSchema, writerSchema);

            var expectedValue = 56723234L;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValue, reader.Read(decoder));
            }
        }

        [TestCase]
        public void UnionToUnionMixedTest()
        {
            var writerSchema = new UnionSchema(new BooleanSchema(), new LongSchema(), new DoubleSchema(), new StringSchema());
            var readerSchema = new UnionSchema(new NullSchema(), new IntSchema(), new FloatSchema(), new BytesSchema());
            var writer = new SpecificWriter<object>(writerSchema);
            var reader = new SpecificReader<object>(readerSchema, writerSchema);

            var expectedValue = 56723234L;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedValue, reader.Read(decoder));
            }
        }

        [TestCase]
        public void ArrayTest()
        {
            var writerSchema = new ArraySchema(new IntSchema());
            var readerSchema = new ArraySchema(new LongSchema());
            var writer = new SpecificWriter<IList<int>>(writerSchema);
            var reader = new SpecificReader<IList<long>>(readerSchema, writerSchema);

            var expectedArray = new List<int> { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };

            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedArray);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedArray, reader.Read(decoder));

            }
        }

        [TestCase]
        public void MapTest()
        {
            var writerSchema = new MapSchema(new FloatSchema());
            var readerSchema = new MapSchema(new DoubleSchema());
            var writer = new SpecificWriter<IDictionary<string, float>>(writerSchema);
            var reader = new SpecificReader<IDictionary<string, double>>(readerSchema, writerSchema);

            var expectedMap = new Dictionary<string, float> { { "Key1", 1.1F }, { "Key2", 2.2F }, { "Key3", 3.3F }, { "Key4", 4.4F }, { "Key5", 5.5F } };

            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedMap);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedMap, reader.Read(decoder));

            }
        }

        [TestCase]
        public void UnresolvedSchema()
        {
            var writerSchema = new IntSchema();
            var readerSchema = new BytesSchema();

            Assert.Throws<AvroException>(() => new SpecificWriter<string>(writerSchema));
            Assert.Throws<AvroException>(() => new SpecificReader<IList<byte>>(readerSchema, writerSchema));
        }
    }

    public enum TestEnum { A, B, C }

    public class TestRecord : ISpecificRecord
    {
        private static readonly Schema _SCHEMA = AvroReader.ReadSchema(@"{""name"":""Avro.Test.Specific.TestRecord"",""type"":""record"",""fields"":[{""name"":""FieldA"",""type"":""int""},{""name"":""FieldB"",""type"":""string""},{""name"":""FieldC"",""type"":{""name"":""Avro.Test.Specific.TestSubRecord"",""type"":""record"",""fields"":[{""name"":""FieldD"",""type"":""boolean""}]}},{""name"":""FieldX"",""type"":{""name"":""Avro.Test.Specific.TestEnum"",""type"":""enum"",""symbols"":[""A"",""B"",""C""]}},{""name"":""TestFixed"",""type"":{""name"":""Avro.Test.Specific.TestFixed"",""type"":""fixed"",""size"":40}}]}");

        public Schema Schema { get => _SCHEMA; }

        public int FieldCount { get => 2; }

        public int FieldA { get; set; }

        public string FieldB { get; set; }

        public TestSubRecord FieldC { get; set; }

        public TestEnum FieldX { get; set; }

        public TestFixed TestFixed { get; set; }

        public object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0:
                    return FieldA;
                case 1:
                    return FieldB;
                case 2:
                    return FieldC;
                case 3:
                    return FieldX;
                case 4:
                    return TestFixed;
                default:
                    throw new AvroException("Bad index " + fieldPos + " in Get()");
            }
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    FieldA = (int)fieldValue;
                    break;
                case 1:
                    FieldB = (string)fieldValue;
                    break;
                case 2:
                    FieldC = (TestSubRecord)fieldValue;
                    break;
                case 3:
                    FieldX = (TestEnum)fieldValue;
                    break;
                case 4:
                    TestFixed = (TestFixed)fieldValue;
                    break;
                default:
                    throw new AvroException("Bad index " + fieldPos + " in Put()");
            }
        }
    }

    public class TestSubRecord : ISpecificRecord
    {
        private static readonly Schema _SCHEMA = AvroReader.ReadSchema(@"{""name"":""Avro.Test.Specific.TestSubRecord"",""type"":""record"",""fields"":[{""name"":""FieldD"",""type"":""boolean""}]}");

        public Schema Schema { get => _SCHEMA; }

        public int FieldCount { get => 2; }

        public bool FieldD { get; set; }

        public object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0:
                    return FieldD;
                default:
                    throw new AvroException("Bad index " + fieldPos + " in Get()");
            }
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    FieldD = (bool)fieldValue;
                    break;
                default:
                    throw new AvroException("Bad index " + fieldPos + " in Put()");
            }
        }
    }

    public class TestFixed : ISpecificFixed
    {
        private static readonly Schema _SCHEMA = AvroReader.ReadSchema(@"{""name"":""Avro.Test.Specific.TestFixed"",""type"":""fixed"",""size"":40}");
        private readonly byte[] _value = new byte[40];
        public Schema Schema => _SCHEMA;
        public int FixedSize => 40;
        public byte[] Value => _value;
    }
}
