using Avro;
using Avro.IO;
using Avro.Schema;
using Avro.Types;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Test.Avro.Resolution
{
    [TestFixture]
    public class ReadWriteAdvTest
    {
        [TestCase]
        public void NullTest()
        {
            var nullSchema = new NullSchema();
            var writer = new DatumWriter<AvroNull>(nullSchema);
            var reader = new DatumReader<AvroNull>(nullSchema, nullSchema);

            var expectedValue = AvroNull.Value;

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValue);
            stream.Seek(0, SeekOrigin.Begin);
            var actualValue = reader.Read(decoder);
            Assert.AreEqual(expectedValue, actualValue);
        }


        [TestCase]
        public void EnumTest()
        {
            var enumSchema = new EnumSchema(nameof(TestEnum), typeof(TestEnum).Namespace ?? string.Empty, Enum.GetNames(typeof(TestEnum)));
            var writer = new DatumWriter<TestEnum>(enumSchema);
            var reader = new DatumReader<TestEnum>(enumSchema, enumSchema);

            var expectedValue = TestEnum.A;

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValue);
            stream.Seek(0, SeekOrigin.Begin);
            var actualValue = reader.Read(decoder);
            Assert.AreEqual(expectedValue, actualValue);
        }

        [TestCase]
        public void EnumGenericTest()
        {
            var enumSchema = AvroParser.ReadSchema<EnumSchema>(@"{""name"":""Test.Avro.Generic.TestEnum"",""type"":""enum"",""symbols"":[""A"",""B"",""C""]}");

            var writer = new DatumWriter<GenericEnum>(enumSchema);
            var reader = new DatumReader<GenericEnum>(enumSchema, enumSchema);

            var expectedValue = new GenericEnum(enumSchema, "B");

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValue);
            stream.Seek(0, SeekOrigin.Begin);
            var actualValue = reader.Read(decoder);
            Assert.AreEqual(expectedValue, actualValue);

            var expectedPosition = stream.Position;
            stream.Seek(0, SeekOrigin.Begin);
            reader.Skip(decoder);
            Assert.AreEqual(expectedPosition, stream.Position);
        }

        [TestCase]
        public void RecordTest()
        {
            var expectedValue = new TestRecord() { FieldA = 123, FieldB = "Test", FieldC = new TestSubRecord() { FieldD = false }, FieldX = TestEnum.B, TestFixed = new TestFixed() };
            var writer = new DatumWriter<TestRecord>(expectedValue.Schema);
            var reader = new DatumReader<TestRecord>(expectedValue.Schema, expectedValue.Schema);

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValue);
            stream.Seek(0, SeekOrigin.Begin);
            var actualValue = reader.Read(decoder);
            for (int i = 0; i < expectedValue.FieldCount; i++)
                Assert.AreEqual(expectedValue[i], actualValue[i]);
        }

        [TestCase]
        public void RecordGenericTest()
        {
            var enumSchema = AvroParser.ReadSchema<EnumSchema>(@"{""name"":""Test.Avro.Generic.TestEnum"",""type"":""enum"",""symbols"":[""A"",""B"",""C""]}");
            var fixedSchema = AvroParser.ReadSchema<FixedSchema>(@"{""name"":""Test.Avro.Generic.TestFixed"",""type"":""fixed"",""size"":40}");
            var subRecordSchema = AvroParser.ReadSchema<RecordSchema>(@"{""name"":""Test.Avro.Generic.TestSubRecord"",""type"":""record"",""fields"":[{""name"":""FieldD"",""type"":""boolean""}]}");
            var recordSchema = AvroParser.ReadSchema<RecordSchema>(@"{""name"":""Test.Avro.Generic.TestRecord"",""type"":""record"",""fields"":[{""name"":""FieldA"",""type"":""int""},{""name"":""FieldB"",""type"":""string""},{""name"":""FieldC"",""type"":{""name"":""Test.Avro.Generic.TestSubRecord"",""type"":""record"",""fields"":[{""name"":""FieldD"",""type"":""boolean""}]}},{""name"":""FieldX"",""type"":{""name"":""Test.Avro.Generic.TestEnum"",""type"":""enum"",""symbols"":[""A"",""B"",""C""]}},{""name"":""TestFixed"",""type"":{""name"":""Test.Avro.Generic.TestFixed"",""type"":""fixed"",""size"":40}}]}");

            var subRecordInstance = new GenericRecord(subRecordSchema);
            var fixedInstance = new GenericFixed(fixedSchema);
            var enumInstance = new GenericEnum(enumSchema, "B");

            subRecordInstance["FieldD"] = false;

            var expectedValue = new GenericRecord(recordSchema);
            expectedValue["FieldA"] = 123;
            expectedValue["FieldB"] = "Test";
            expectedValue["FieldC"] = subRecordInstance;
            expectedValue["FieldX"] = enumInstance;
            expectedValue["TestFixed"] = fixedInstance;

            var writer = new DatumWriter<GenericRecord>(recordSchema);
            var reader = new DatumReader<GenericRecord>(recordSchema, recordSchema);

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValue);
            stream.Seek(0, SeekOrigin.Begin);
            var actualValue = reader.Read(decoder);
            Assert.AreEqual(expectedValue, actualValue);
        }

        [TestCase]
        public void RecordTestWithOverlap()
        {
            var expectedValue = new TestRecordWithDefault() { Name = "Test" };
            var writeValue = new TestRecordWithExtraField() { Name = "Test", Desc = "Description" };
            var writer = new DatumWriter<TestRecordWithExtraField>(writeValue.Schema);
            var reader = new DatumReader<TestRecordWithDefault>(expectedValue.Schema, writeValue.Schema);

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, writeValue);
            stream.Seek(0, SeekOrigin.Begin);
            var actualValue = reader.Read(decoder);
            for (int i = 0; i < expectedValue.FieldCount; i++)
                Assert.AreEqual(expectedValue[i], actualValue[i]);
        }

        [TestCase]
        public void RecordTestWithoutOverlap()
        {
            var expectedValue = new TestRecordWithoutDefault() { ID = 123, Name = "Test" };
            var writeValue = new TestRecordWithExtraField() { Name = "Test", Desc = "Description" };
            var writer = new DatumWriter<TestRecordWithExtraField>(writeValue.Schema);
            Assert.Throws<ArgumentException>(() => new DatumReader<TestRecordWithDefault>(expectedValue.Schema, writeValue.Schema));
        }

        [TestCase]
        public void FixedTest()
        {
            var expectedValue = new TestFixed();
            expectedValue[1] = 1;

            for (int i = 2; i < expectedValue.Size; i++)
                expectedValue[i] = (byte)((expectedValue[i - 2] + expectedValue[i - 1]) % byte.MaxValue);

            var writer = new DatumWriter<TestFixed>(expectedValue.Schema);
            var reader = new DatumReader<TestFixed>(expectedValue.Schema, expectedValue.Schema);

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValue);
            stream.Seek(0, SeekOrigin.Begin);
            var actualValue = reader.Read(decoder);
            Assert.AreEqual(expectedValue, actualValue);
        }

        [TestCase]
        public void NullableValueTest()
        {
            var writerSchema = new UnionSchema(new FloatSchema(), new NullSchema());
            var readerSchema = new UnionSchema(new NullSchema(), new FloatSchema());
            var writer = new DatumWriter<float?>(writerSchema);
            var reader = new DatumReader<float?>(readerSchema, writerSchema);

            var expectedValueNotNull = new float?(56.45F);
            var expectedValueNull = new float?();

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValueNotNull);

            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedValueNotNull, reader.Read(decoder));
            stream.Seek(0, SeekOrigin.Begin);
            writer.Write(encoder, expectedValueNull);
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedValueNull, reader.Read(decoder));
        }

        [TestCase]
        public void NullableReferenceTest()
        {
            var writerSchema = new UnionSchema(new StringSchema(), new NullSchema());
            var readerSchema = new UnionSchema(new NullSchema(), new StringSchema());
            var writer = new DatumWriter<string?>(writerSchema);
            var reader = new DatumReader<string?>(readerSchema, writerSchema);

            var expectedValueNotNull = "Some String";
            var expectedValueNull = null as string;

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValueNotNull);
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedValueNotNull, reader.Read(decoder));
            stream.Seek(0, SeekOrigin.Begin);
            writer.Write(encoder, expectedValueNull);
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedValueNull, reader.Read(decoder));
        }

        [TestCase]
        public void NullableNullWriterTest()
        {
            var writerSchema = new NullSchema();
            var readerSchema = new UnionSchema(new NullSchema(), new StringSchema());
            var writer = new DatumWriter<string?>(writerSchema);
            var reader = new DatumReader<string?>(readerSchema, writerSchema);

            var expectedValueNotNull = "Some String";
            var expectedValueNull = (string?)null;

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValueNotNull);
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedValueNull, reader.Read(decoder));
            stream.Seek(0, SeekOrigin.Begin);
            writer.Write(encoder, expectedValueNull);
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedValueNull, reader.Read(decoder));
        }

        [TestCase]
        public void NullableNotNullWriterTest()
        {
            var writerSchema = new IntSchema();
            var readerSchema = new UnionSchema(new NullSchema(), new IntSchema());
            var writer = new DatumWriter<int>(writerSchema);
            var reader = new DatumReader<int?>(readerSchema, writerSchema);

            var expectedValueNotNull = 123;

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValueNotNull);
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedValueNotNull, reader.Read(decoder));
            stream.Seek(0, SeekOrigin.Begin);
        }

        [TestCase]
        public void NullableArbitraryUnionTest()
        {
            var writerSchema = new UnionSchema(new StringSchema(), new DoubleSchema(), new UuidSchema(), new NullSchema());
            var readerSchema = new UnionSchema(new StringSchema(), new NullSchema());
            var writer = new DatumWriter<AvroUnion<string, double, Guid, AvroNull>>(writerSchema);
            var reader = new DatumReader<string?>(readerSchema, writerSchema);

            var expectedValueNotNull = "Some String";
            var expectedValueNull = (string?)null;

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, new AvroUnion<string, double, Guid, AvroNull>("Some String"));
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedValueNotNull, reader.Read(decoder));
            stream.Seek(0, SeekOrigin.Begin);
            writer.Write(encoder, new AvroUnion<string, double, Guid, AvroNull>(AvroNull.Value));
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedValueNull, reader.Read(decoder));
        }


        [TestCase]
        public void NonUnionReferenceToUnionTest()
        {
            var writerSchema = new StringSchema();
            var readerSchema = new UnionSchema(new FloatSchema(), new StringSchema(), new BytesSchema());
            var writer = new DatumWriter<string>(writerSchema);
            var reader = new DatumReader<AvroUnion<float, string, byte[]>>(readerSchema, writerSchema);

            var expectedValue = new AvroUnion<float, string, byte[]>("Test String");

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValue);
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedValue, reader.Read(decoder));
        }

        [TestCase]
        public void NonUnionValueToUnionTest()
        {
            var writerSchema = new FloatSchema();
            var readerSchema = new UnionSchema(new FloatSchema(), new StringSchema(), new BytesSchema());
            var writer = new DatumWriter<float>(writerSchema);
            var reader = new DatumReader<AvroUnion<float, string, byte[]>>(readerSchema, writerSchema);

            var expectedValue = new AvroUnion<float, string, byte[]>(123.456F);

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValue);
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedValue, reader.Read(decoder));
        }

        [TestCase]
        public void UnionToNonUnionTest()
        {
            var writerSchema = new UnionSchema(new BooleanSchema(), new LongSchema(), new DoubleSchema(), new StringSchema());
            var readerSchema = new LongSchema();
            var writer = new DatumWriter<AvroUnion<bool, long, double, string>>(writerSchema);
            var reader = new DatumReader<long>(readerSchema, writerSchema);

            var expectedValue = new AvroUnion<bool, long, double, string>(56723234L);

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValue);
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedValue, reader.Read(decoder));
        }

        [TestCase]
        public void UnionToUnionMixedTest()
        {
            var writerSchema = new UnionSchema(new BooleanSchema(), new LongSchema(), new DoubleSchema(), new StringSchema());
            var readerSchema = new UnionSchema(new NullSchema(), new IntSchema(), new FloatSchema(), new BytesSchema());
            var writer = new DatumWriter<AvroUnion<bool, long, double, string>>(writerSchema);
            var reader = new DatumReader<AvroUnion<AvroNull, int, float, byte[]>>(readerSchema, writerSchema);

            var expectedValue = new AvroUnion<bool, long, double, string>(56723234L);

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            writer.Write(encoder, expectedValue);
            stream.Seek(0, SeekOrigin.Begin);
            var actualValue = reader.Read(decoder);
            Assert.AreEqual(expectedValue.Value, actualValue.Value);
        }

        [TestCase]
        public void ArrayTest()
        {
            var writerSchema = new ArraySchema(new IntSchema());
            var readerSchema = new ArraySchema(new LongSchema());
            var writer = new DatumWriter<IList<int>>(writerSchema);
            var reader = new DatumReader<IList<long>>(readerSchema, writerSchema);

            var expectedArray = new List<int> { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            stream.Seek(0, SeekOrigin.Begin);
            writer.Write(encoder, expectedArray);
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedArray, reader.Read(decoder));
        }

        [TestCase]
        public void MapTest()
        {
            var writerSchema = new MapSchema(new FloatSchema());
            var readerSchema = new MapSchema(new DoubleSchema());
            var writer = new DatumWriter<IDictionary<string, float>>(writerSchema);
            var reader = new DatumReader<IDictionary<string, double>>(readerSchema, writerSchema);

            var expectedMap = new Dictionary<string, float> { { "Key1", 1.1F }, { "Key2", 2.2F }, { "Key3", 3.3F }, { "Key4", 4.4F }, { "Key5", 5.5F } };

            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            stream.Seek(0, SeekOrigin.Begin);
            writer.Write(encoder, expectedMap);
            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(expectedMap, reader.Read(decoder));
        }

        [TestCase]
        public void UnresolvedSchema()
        {
            var writerSchema = new IntSchema();
            var readerSchema = new BytesSchema();

            Assert.Throws<InvalidOperationException>(() => new DatumWriter<string>(writerSchema));
            Assert.Throws<AvroException>(() => new DatumReader<IList<byte>>(readerSchema, writerSchema));
        }
    }

    public enum TestEnum { A, B, C }

    public class TestRecord : IAvroRecord
    {
        private static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema<RecordSchema>(@$"{{""name"":""{typeof(TestRecord).Namespace}.{typeof(TestRecord).Name}"",""type"":""record"",""fields"":[{{""name"":""FieldA"",""type"":""int""}},{{""name"":""FieldB"",""type"":""string""}},{{""name"":""FieldC"",""type"":{{""name"":""{typeof(TestSubRecord).Namespace}.{typeof(TestSubRecord).Name}"",""type"":""record"",""fields"":[{{""name"":""FieldD"",""type"":""boolean""}}]}}}},{{""name"":""FieldX"",""type"":{{""name"":""{typeof(TestEnum).Namespace}.{typeof(TestEnum).Name}"",""type"":""enum"",""symbols"":[""A"",""B"",""C""]}}}},{{""name"":""TestFixed"",""type"":{{""name"":""{typeof(TestFixed).Namespace}.{typeof(TestFixed).Name}"",""type"":""fixed"",""size"":40}}}}]}}");

        public RecordSchema Schema { get => _SCHEMA; }

        public int FieldCount { get => 2; }

        public int FieldA { get; set; }

        public string FieldB { get; set; } = string.Empty;

        public TestSubRecord FieldC { get; set; } = new TestSubRecord();

        public TestEnum FieldX { get; set; }

        public TestFixed TestFixed { get; set; } = new TestFixed();

        public object? this[int i]
        {
            get => i switch
            {
                0 => FieldA,
                1 => FieldB,
                2 => FieldC,
                3 => FieldX,
                4 => TestFixed,
                _ => throw new AvroException("Bad index " + i + " in Get()"),
            };
            set
            {
                switch (i)
                {
                    case 0:
                        FieldA = (int)(value ?? 0);
                        break;
                    case 1:
                        FieldB = (string)(value ?? string.Empty);
                        break;
                    case 2:
                        FieldC = (TestSubRecord)(value ?? new TestSubRecord());
                        break;
                    case 3:
                        FieldX = (TestEnum)(value ?? default(TestEnum));
                        break;
                    case 4:
                        TestFixed = (TestFixed)(value ?? new TestFixed());
                        break;
                    default:
                        throw new AvroException("Bad index " + value + " in Put()");
                }
            }
        }
    }

    public class TestSubRecord : IAvroRecord
    {
        private static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema<RecordSchema>(@$"{{""name"":""{typeof(TestSubRecord).Namespace}.{typeof(TestSubRecord).Name}"",""type"":""record"",""fields"":[{{""name"":""FieldD"",""type"":""boolean""}}]}}");

        public RecordSchema Schema { get => _SCHEMA; }

        public int FieldCount { get => 1; }

        public bool FieldD { get; set; }

        public object? this[int i]
        {
            get => i switch
            {
                0 => FieldD,
                _ => throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]"),
            };
            set
            {
                switch (i)
                {
                    case 0:
                        FieldD = (bool)(value ?? false);
                        break;
                    default:
                        throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]");
                }
            }
        }
    }

    public class TestFixed : IAvroFixed
    {
        private static readonly FixedSchema _SCHEMA = AvroParser.ReadSchema<FixedSchema>(@$"{{""name"":""{typeof(TestFixed).Namespace}.{typeof(TestFixed).Name}"",""type"":""fixed"",""size"":40}}");
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
        public byte[] Value => _value;
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
        private static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema<RecordSchema>(@$"{{""name"":""{typeof(TestRecordWithDefault).Namespace}.{typeof(TestRecordWithDefault).Name}"",""type"":""record"",""fields"":[{{""name"":""ID"",""type"":""int"",""default"":-1}},{{""name"":""Name"",""type"":""string""}}]}}");

        public RecordSchema Schema { get => _SCHEMA; }

        public int FieldCount { get => 2; }

        public int ID { get; set; } = -1;

        public string Name { get; set; } = string.Empty;

        public object? this[int i]
        {
            get => i switch
            {
                0 => ID,
                1 => Name,
                _ => throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]"),
            };
            set
            {
                switch (i)
                {
                    case 0:
                        ID = (int)(value ?? 0);
                        break;
                    case 1:
                        Name = (string)(value ?? string.Empty);
                        break;
                    default:
                        throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]");
                }
            }
        }
    }

    public class TestRecordWithoutDefault : IAvroRecord
    {
        private static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema<RecordSchema>(@$"{{""name"":""{typeof(TestRecordWithoutDefault).Namespace}.{typeof(TestRecordWithoutDefault).Name}"",""type"":""record"",""fields"":[{{""name"":""ID"",""type"":""int""}},{{""name"":""Name"",""type"":""string""}}]}}");

        public RecordSchema Schema { get => _SCHEMA; }

        public int FieldCount { get => 2; }

        public int ID { get; set; }

        public string Name { get; set; } = string.Empty;

        public object? this[int i]
        {
            get => i switch
            {
                0 => ID,
                1 => Name,
                _ => throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]"),
            };
            set
            {
                switch (i)
                {
                    case 0:
                        ID = (int)(value ?? 0);
                        break;
                    case 1:
                        Name = (string)(value ?? string.Empty);
                        break;
                    default:
                        throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]");
                }
            }
        }
    }

    public class TestRecordWithExtraField : IAvroRecord
    {
        private static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema<RecordSchema>(@$"{{""name"":""{typeof(TestRecordWithDefault).Namespace}.{typeof(TestRecordWithDefault).Name}"",""type"":""record"",""fields"":[{{""name"":""Name"",""type"":""string""}},{{""name"":""Desc"",""type"":""string""}}]}}");

        public RecordSchema Schema { get => _SCHEMA; }

        public int FieldCount { get => 2; }

        public string Name { get; set; } = string.Empty;

        public string Desc { get; set; } = string.Empty;

        public object? this[int i]
        {
            get => i switch
            {
                0 => Name,
                1 => Desc,
                _ => throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]"),
            };
            set
            {
                switch (i)
                {
                    case 0:
                        Name = (string)(value ?? string.Empty);
                        break;
                    case 1:
                        Desc = (string)(value ?? string.Empty);
                        break;
                    default:
                        throw new IndexOutOfRangeException($"Valid range [0:{FieldCount}]");
                }
            }
        }
    }
}
