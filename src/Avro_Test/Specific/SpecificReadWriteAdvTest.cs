using Avro;
using Avro.IO;
using Avro.Schemas;
using Avro.Specific;
using NUnit.Framework;
using System;
using System.IO;
using System.Text;

namespace Tests.Specific
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
            var expectedValue = new TestRecord() { FieldA = 123, FieldB = "Test", FieldC = new TestSubRecord() { FieldD = false }, FieldX = TestEnum.B };
            var writer = new SpecificWriter<TestRecord>(expectedValue.Schema);
            var reader = new SpecificReader<TestRecord>(expectedValue.Schema, expectedValue.Schema);

            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = reader.Read(decoder);
                for(int i = 0 ; i < expectedValue.FieldCount; i++)
                    Assert.AreEqual(expectedValue.Get(i), actualValue.Get(i));
            }
        }

        [TestCase]
        public void NullableFloatTest()
        {
            var writerSchema = new UnionSchema(new NullSchema(), new FloatSchema(), new StringSchema());
            var readerSchema = new UnionSchema(new NullSchema(), new FloatSchema());
            var writer = new SpecificWriter<object>(writerSchema);
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
                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, "This throws exception on reader");
                stream.Seek(0, SeekOrigin.Begin);
                Assert.Throws(typeof(InvalidCastException), () => { reader.Read(decoder); });
            }
        }


        [TestCase]
        public void NonUnionToUnionTest()
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
        public void UnionToNonUnionTest()
        {
            var writerSchema = new UnionSchema(new FloatSchema(), new StringSchema(), new BytesSchema());
            var readerSchema = new StringSchema();
            var writer = new SpecificWriter<object>(writerSchema);
            var reader = new SpecificReader<string>(readerSchema, writerSchema);

            var expectedString = "Test String";
            var byteValue =  Encoding.UTF8.GetBytes("Test String");
            var floatValue = 123.765F;
            var writeErrorValue = TestEnum.B;
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, expectedString);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedString, reader.Read(decoder));

                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, byteValue);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.AreEqual(expectedString, reader.Read(decoder));

                stream.Seek(0, SeekOrigin.Begin);
                writer.Write(encoder, floatValue);
                stream.Seek(0, SeekOrigin.Begin);
                Assert.Throws(typeof(InvalidCastException), () => reader.Read(decoder));

                stream.Seek(0, SeekOrigin.Begin);
                Assert.Throws(typeof(ArgumentException), () => writer.Write(encoder, writeErrorValue));

            }
        }
    }

    public enum TestEnum { A, B, C }

    public class TestRecord : ISpecificRecord
    {
        private static readonly Schema _SCHEMA = Schema.Parse(@"{""name"":""Tests.Specific.TestRecord"",""type"":""record"",""fields"":[{""name"":""FieldA"",""type"":""int""},{""name"":""FieldB"",""type"":""string""},{""name"":""FieldC"",""type"":{""name"":""Tests.Specific.TestSubRecord"",""type"":""record"",""fields"":[{""name"":""FieldD"",""type"":""boolean""}]}},{""name"":""FieldX"",""type"":{""name"":""Tests.Specific.TestEnum"",""type"":""enum"",""symbols"":[""A"",""B"",""C""]}}]}");

        public Schema Schema { get => _SCHEMA; }

        public int FieldCount { get => 2; }

        public int FieldA { get; set; }

        public string FieldB { get; set; }

        public TestSubRecord FieldC { get; set; }

        public TestEnum FieldX { get; set; }

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
                default:
                    throw new AvroException("Bad index " + fieldPos + " in Put()");
            }
        }
    }

    public class TestSubRecord : ISpecificRecord
    {
        private static readonly Schema _SCHEMA = Schema.Parse(@"{""name"":""Tests.Specific.TestSubRecord"",""type"":""record"",""fields"":[{""name"":""FieldD"",""type"":""boolean""}]}");

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
}
