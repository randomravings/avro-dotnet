using Avro.IO;
using Avro.Schemas;
using Avro.Specific;
using Avro.Types;
using NUnit.Framework;
using System;
using System.Collections;
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
        public void EnumGenericTest()
        {
            var enumSchema = AvroParser.ReadSchema<EnumSchema>(@"{""name"":""Avro.Test.Generic.TestEnum"",""type"":""enum"",""symbols"":[""A"",""B"",""C""]}");

            var writer = new SpecificWriter<GenericAvroEnum>(enumSchema);
            var reader = new SpecificReader<GenericAvroEnum>(enumSchema, enumSchema);

            var expectedValue = new GenericAvroEnum(enumSchema, "B");
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
        public void RecordGenericTest()
        {
            var enumSchema = AvroParser.ReadSchema<EnumSchema>(@"{""name"":""Avro.Test.Generic.TestEnum"",""type"":""enum"",""symbols"":[""A"",""B"",""C""]}");
            var fixedSchema = AvroParser.ReadSchema<FixedSchema>(@"{""name"":""Avro.Test.Generic.TestFixed"",""type"":""fixed"",""size"":40}");
            var subRecordSchema = AvroParser.ReadSchema<RecordSchema>(@"{""name"":""Avro.Test.Generic.TestSubRecord"",""type"":""record"",""fields"":[{""name"":""FieldD"",""type"":""boolean""}]}");
            var recordSchema = AvroParser.ReadSchema<RecordSchema>(@"{""name"":""Avro.Test.Generic.TestRecord"",""type"":""record"",""fields"":[{""name"":""FieldA"",""type"":""int""},{""name"":""FieldB"",""type"":""string""},{""name"":""FieldC"",""type"":{""name"":""Avro.Test.Generic.TestSubRecord"",""type"":""record"",""fields"":[{""name"":""FieldD"",""type"":""boolean""}]}},{""name"":""FieldX"",""type"":{""name"":""Avro.Test.Generic.TestEnum"",""type"":""enum"",""symbols"":[""A"",""B"",""C""]}},{""name"":""TestFixed"",""type"":{""name"":""Avro.Test.Generic.TestFixed"",""type"":""fixed"",""size"":40}}]}");

            var subRecordInstance = new GenericAvroRecord(subRecordSchema);
            var fixedInstance = new GenericAvroFixed(fixedSchema);
            var enumInstance = new GenericAvroEnum(enumSchema, "B");

            subRecordInstance["FieldD"] = false;

            var expectedValue = new GenericAvroRecord(recordSchema);
            expectedValue["FieldA"] = 123;
            expectedValue["FieldB"] = "Test";
            expectedValue["FieldC"] = subRecordInstance;
            expectedValue["FieldX"] = enumInstance;
            expectedValue["TestFixed"] = fixedInstance;

            var writer = new SpecificWriter<GenericAvroRecord>(recordSchema);
            var reader = new SpecificReader<GenericAvroRecord>(recordSchema, recordSchema);

            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                writer.Write(encoder, expectedValue);
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = reader.Read(decoder);
                for (int i = 0; i < expectedValue.FieldCount; i++)
                    Assert.AreEqual(expectedValue[i], actualValue[i]);
            }
        }

        [TestCase]
        public void RecordTestWithOverlap()
        {
            var expectedValue = new TestRecordWithDefault() { Name = "Test" };
            var writeValue = new TestRecordWithExtraField() { Name = "Test", Desc = "Description" };
            var writer = new SpecificWriter<TestRecordWithExtraField>(writeValue.Schema);
            var reader = new SpecificReader<TestRecordWithDefault>(expectedValue.Schema, writeValue.Schema);

            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                writer.Write(encoder, writeValue);
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = reader.Read(decoder);
                for (int i = 0; i < expectedValue.FieldCount; i++)
                    Assert.AreEqual(expectedValue.Get(i), actualValue.Get(i));
            }
        }

        [TestCase]
        public void RecordTestWithoutOverlap()
        {
            var expectedValue = new TestRecordWithoutDefault() { ID = 123, Name = "Test" };
            var writeValue = new TestRecordWithExtraField() { Name = "Test", Desc = "Description" };
            var writer = new SpecificWriter<TestRecordWithExtraField>(writeValue.Schema);
            Assert.Throws<AvroException>(() => new SpecificReader<TestRecordWithDefault>(expectedValue.Schema, writeValue.Schema));
        }

        [TestCase]
        public void FixedTest()
        {
            var expectedValue = new TestFixed();
            expectedValue[1] = 1;

            for (int i = 2; i < expectedValue.Size; i++)
                expectedValue[i] = (byte)((expectedValue[i - 2] + expectedValue[i - 1]) % byte.MaxValue);

            var writer = new SpecificWriter<TestFixed>(expectedValue.Schema);
            var reader = new SpecificReader<TestFixed>(expectedValue.Schema, expectedValue.Schema);

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

    public class TestRecord : IAvroRecord
    {
        private static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema(@"{""name"":""Avro.Test.Specific.TestRecord"",""type"":""record"",""fields"":[{""name"":""FieldA"",""type"":""int""},{""name"":""FieldB"",""type"":""string""},{""name"":""FieldC"",""type"":{""name"":""Avro.Test.Specific.TestSubRecord"",""type"":""record"",""fields"":[{""name"":""FieldD"",""type"":""boolean""}]}},{""name"":""FieldX"",""type"":{""name"":""Avro.Test.Specific.TestEnum"",""type"":""enum"",""symbols"":[""A"",""B"",""C""]}},{""name"":""TestFixed"",""type"":{""name"":""Avro.Test.Specific.TestFixed"",""type"":""fixed"",""size"":40}}]}") as RecordSchema;

        public RecordSchema Schema { get => _SCHEMA; }

        public int FieldCount { get => 2; }

        public int FieldA { get; set; }

        public string FieldB { get; set; }

        public TestSubRecord FieldC { get; set; }

        public TestEnum FieldX { get; set; }

        public TestFixed TestFixed { get; set; }

        public object this[int i]
        {
            get
            {
                switch (i)
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
                        throw new AvroException("Bad index " + i + " in Get()");
                }
            }
            set
            {
                switch (i)
                {
                    case 0:
                        FieldA = (int)value;
                        break;
                    case 1:
                        FieldB = (string)value;
                        break;
                    case 2:
                        FieldC = (TestSubRecord)value;
                        break;
                    case 3:
                        FieldX = (TestEnum)value;
                        break;
                    case 4:
                        TestFixed = (TestFixed)value;
                        break;
                    default:
                        throw new AvroException("Bad index " + value + " in Put()");
                }
            }
                }

        public object Get(int fieldPos)
        {
            return this[fieldPos];
        }

        public void Put(int fieldPos, object fieldValue)
        {
            this[fieldPos] = fieldValue;
        }
    }

    public class TestSubRecord : IAvroRecord
    {
        private static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema(@"{""name"":""Avro.Test.Specific.TestSubRecord"",""type"":""record"",""fields"":[{""name"":""FieldD"",""type"":""boolean""}]}") as RecordSchema;

        public RecordSchema Schema { get => _SCHEMA; }

        public int FieldCount { get => 1; }

        public bool FieldD { get; set; }

        public object this[int i]
        {
            get
            {
                switch (i)
                {
                    case 0:
                        return FieldD;
                    default:
                        throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]");
                }
            }
            set
            {
                switch (i)
                {
                    case 0:
                        FieldD = (bool)value;
                        break;
                    default:
                        throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]");
                }
            }
        }

        public object Get(int fieldPos)
        {
            return this[fieldPos];
        }

        public void Put(int fieldPos, object fieldValue)
        {
            this[fieldPos] = fieldValue;
        }
    }

    public class TestFixed : IAvroFixed
    {
        private static readonly FixedSchema _SCHEMA = AvroParser.ReadSchema(@"{""name"":""Avro.Test.Specific.TestFixed"",""type"":""fixed"",""size"":40}") as FixedSchema;
        public const int _SIZE = 40;
        private readonly byte[] _value;
        public FixedSchema Schema => _SCHEMA;
        public int Size => _SIZE;
        public TestFixed()
        {
            _value = new byte[_SIZE];
        }

        public TestFixed(byte[] value)
        {
            if (value.Length != _SIZE)
                throw new ArgumentException($"Array must be of size: {_SIZE}");
            _value = value;
        }
        public byte this[int i] { get => _value[i]; set => _value[i] = value; }
        public static implicit operator TestFixed(byte[] value) => new TestFixed(value);
        public static explicit operator byte[](TestFixed value) => value._value;

        public bool Equals(IAvroFixed other)
        {
            if (Size != other.Size)
                return false;
            for (int i = 0; i < Size; i++)
                if (this[i] != other[i])
                    return false;
            return true;
        }

        public IEnumerator<byte> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
    
    public class TestRecordWithDefault : IAvroRecord
    {
        private static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema(@"{""name"":""Avro.Test.Specific.TestRecordWithDefault"",""type"":""record"",""fields"":[{""name"":""ID"",""type"":""int"",""default"":-1},{""name"":""Name"",""type"":""string""}]}") as RecordSchema;

        public RecordSchema Schema { get => _SCHEMA; }

        public int FieldCount { get => 2; }

        public int ID { get; set; } = -1;

        public string Name { get; set; }

        public object this[int i]
        {
            get
            {
                switch (i)
                {
                    case 0:
                        return ID;
                    case 1:
                        return Name;
                    default:
                        throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]");
                }
            }
            set
            {
                switch (i)
                {
                    case 0:
                        ID = (int)value;
                        break;
                    case 1:
                        Name = (string)value;
                        break;
                    default:
                        throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]");
                }
            }
        }

        public object Get(int fieldPos)
        {
            return this[fieldPos];
        }

        public void Put(int fieldPos, object fieldValue)
        {
            this[fieldPos] = fieldValue;
        }
    }

    public class TestRecordWithoutDefault : IAvroRecord
    {
        private static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema(@"{""name"":""Avro.Test.Specific.TestRecordWithDefault"",""type"":""record"",""fields"":[{""name"":""ID"",""type"":""int""},{""name"":""Name"",""type"":""string""}]}") as RecordSchema;

        public RecordSchema Schema { get => _SCHEMA; }

        public int FieldCount { get => 2; }

        public int ID { get; set; }

        public string Name { get; set; }

        public object this[int i]
        {
            get
            {
                switch (i)
                {
                    case 0:
                        return ID;
                    case 1:
                        return Name;
                    default:
                        throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]");
                }
            }
            set
            {
                switch (i)
                {
                    case 0:
                        ID = (int)value;
                        break;
                    case 1:
                        Name = (string)value;
                        break;
                    default:
                        throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]");
                }
            }
        }

        public object Get(int fieldPos)
        {
            return this[fieldPos];
        }

        public void Put(int fieldPos, object fieldValue)
        {
            this[fieldPos] = fieldValue;
        }
    }

    public class TestRecordWithExtraField : IAvroRecord
    {
        private static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema(@"{""name"":""Avro.Test.Specific.TestRecordWithDefault"",""type"":""record"",""fields"":[{""name"":""Name"",""type"":""string""},{""name"":""Desc"",""type"":""string""}]}") as RecordSchema;

        public RecordSchema Schema { get => _SCHEMA; }

        public int FieldCount { get => 2; }

        public string Name { get; set; }

        public string Desc { get; set; }

        public object this[int i]
        {
            get
            {
                switch (i)
                {
                    case 0:
                        return Name;
                    case 1:
                        return Desc;
                    default:
                        throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]");
                }
            }
            set
            {
                switch (i)
                {
                    case 0:
                        Name = (string)value;
                        break;
                    case 1:
                        Desc = (string)value;
                        break;
                    default:
                        throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]");
                }
            }
        }

        public object Get(int fieldPos)
        {
            return this[fieldPos];
        }

        public void Put(int fieldPos, object fieldValue)
        {
            this[fieldPos] = fieldValue;
        }
    }
}
