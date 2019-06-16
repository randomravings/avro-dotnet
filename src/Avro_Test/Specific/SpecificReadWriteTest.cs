using Avro;
using Avro.IO;
using Avro.Schemas;
using Avro.Specific;
using NUnit.Framework;
using System;
using System.IO;

namespace Tests.Specific
{
    public class SpecificReadWriteTest<TWriteType, TWriteSchema, TReadType, TReadSchema> where TWriteSchema : Schema, new() where TReadSchema : Schema, new()
    {
        private readonly TWriteSchema _writeSchema;
        private readonly TReadSchema _readSchema;
        private readonly SpecificWriter<TWriteType> _specificWriter;
        private readonly SpecificReader<TReadType> _specificReader;

        private readonly TWriteType _writeValue;
        private readonly TReadType _expectedReadValue;

        public SpecificReadWriteTest(TWriteType writeValue, TReadType expectedReadValue)
        {
            _writeSchema = new TWriteSchema();
            _readSchema = new TReadSchema();
            _specificWriter = new SpecificWriter<TWriteType>(_writeSchema);
            _specificReader = new SpecificReader<TReadType>(_readSchema, _writeSchema);
            _writeValue = writeValue;
            _expectedReadValue = expectedReadValue;
        }

        [TestCase]
        public void TestReadWrite()
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {

                _specificWriter.Write(encoder, _writeValue);
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = _specificReader.Read(decoder);
                Assert.AreEqual(_expectedReadValue, actualValue);
            }
        }
    }

    [TestFixture(null, TypeArgs = new Type[] { typeof(object), typeof(NullSchema) })]
    [TestFixture(true, TypeArgs = new Type[] { typeof(bool), typeof(BooleanSchema) })]
    [TestFixture(false, TypeArgs = new Type[] { typeof(bool), typeof(BooleanSchema) })]
    [TestFixture(0, TypeArgs = new Type[] { typeof(int), typeof(IntSchema) })]
    [TestFixture(123, TypeArgs = new Type[] { typeof(int), typeof(IntSchema) })]
    [TestFixture(-123, TypeArgs = new Type[] { typeof(int), typeof(IntSchema) })]
    [TestFixture(int.MinValue, TypeArgs = new Type[] { typeof(int), typeof(IntSchema) })]
    [TestFixture(int.MaxValue, TypeArgs = new Type[] { typeof(int), typeof(IntSchema) })]
    [TestFixture(0L, TypeArgs = new Type[] { typeof(long), typeof(LongSchema) })]
    [TestFixture(4096L, TypeArgs = new Type[] { typeof(long), typeof(LongSchema) })]
    [TestFixture(-4096L, TypeArgs = new Type[] { typeof(long), typeof(LongSchema) })]
    [TestFixture(long.MinValue, TypeArgs = new Type[] { typeof(long), typeof(LongSchema) })]
    [TestFixture(long.MaxValue, TypeArgs = new Type[] { typeof(long), typeof(LongSchema) })]
    [TestFixture(0F, TypeArgs = new Type[] { typeof(float), typeof(FloatSchema) })]
    [TestFixture(0.8548795438F, TypeArgs = new Type[] { typeof(float), typeof(FloatSchema) })]
    [TestFixture(-0.8548795438F, TypeArgs = new Type[] { typeof(float), typeof(FloatSchema) })]
    [TestFixture(2345.543F, TypeArgs = new Type[] { typeof(float), typeof(FloatSchema) })]
    [TestFixture(-2345.543F, TypeArgs = new Type[] { typeof(float), typeof(FloatSchema) })]
    [TestFixture(float.MinValue, TypeArgs = new Type[] { typeof(float), typeof(FloatSchema) })]
    [TestFixture(float.MaxValue, TypeArgs = new Type[] { typeof(float), typeof(FloatSchema) })]
    [TestFixture(0D, TypeArgs = new Type[] { typeof(double), typeof(DoubleSchema) })]
    [TestFixture(0.0000000000234D, TypeArgs = new Type[] { typeof(double), typeof(DoubleSchema) })]
    [TestFixture(-0.0000000000234D, TypeArgs = new Type[] { typeof(double), typeof(DoubleSchema) })]
    [TestFixture(987344587653456.4564D, TypeArgs = new Type[] { typeof(double), typeof(DoubleSchema) })]
    [TestFixture(-987344587653456.4564D, TypeArgs = new Type[] { typeof(double), typeof(DoubleSchema) })]
    [TestFixture(double.MinValue, TypeArgs = new Type[] { typeof(double), typeof(DoubleSchema) })]
    [TestFixture(double.MaxValue, TypeArgs = new Type[] { typeof(double), typeof(DoubleSchema) })]
    [TestFixture("Test String", TypeArgs = new Type[] { typeof(string), typeof(StringSchema) })]
    [TestFixture("... And some Symbols: ¤#%&#?/&", TypeArgs = new Type[] { typeof(string), typeof(StringSchema) })]
    [TestFixture("", TypeArgs = new Type[] { typeof(string), typeof(StringSchema) })]
    [TestFixture(new byte[] { 0x00, 0x00, 0x00 }, TypeArgs = new Type[] { typeof(byte[]), typeof(BytesSchema) })]
    [TestFixture(new byte[] { 0x65, 0xAF, 0x77, 0x12, 0xB4 }, TypeArgs = new Type[] { typeof(byte[]), typeof(BytesSchema) })]
    [TestFixture(new byte[] { }, TypeArgs = new Type[] { typeof(byte[]), typeof(BytesSchema) })]
    public class SpecificReadWriteMirrored<TDataType, TSchemaType> : SpecificReadWriteTest<TDataType, TSchemaType, TDataType, TSchemaType> where TSchemaType : Schema, new()
    {
        public SpecificReadWriteMirrored(TDataType value)
            : base(value, value) { }
    }
}
