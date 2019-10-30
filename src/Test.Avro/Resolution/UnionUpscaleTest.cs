using Avro;
using Avro.IO;
using Avro.Schema;
using Avro.Types;
using NUnit.Framework;
using System;
using System.Collections;
using System.Globalization;
using System.IO;

namespace Test.Avro.Resolution
{
    [TestFixture(123, 123L, TypeArgs = new Type[] { typeof(int), typeof(long), typeof(float), typeof(IntSchema), typeof(LongSchema), typeof(FloatSchema) }, TestName = "1")]
    [TestFixture(456, 456F, TypeArgs = new Type[] { typeof(int), typeof(float), typeof(double), typeof(IntSchema), typeof(FloatSchema), typeof(DoubleSchema) }, TestName = "2")]
    [TestFixture(789, 789D, TypeArgs = new Type[] { typeof(int), typeof(double), typeof(string), typeof(IntSchema), typeof(DoubleSchema), typeof(StringSchema) }, TestName = "3")]
    [TestFixture(1024L, 1024F, TypeArgs = new Type[] { typeof(long), typeof(float), typeof(double), typeof(LongSchema), typeof(FloatSchema), typeof(DoubleSchema) }, TestName = "4")]
    [TestFixture(2048L, 2048D, TypeArgs = new Type[] { typeof(long), typeof(double), typeof(double), typeof(LongSchema), typeof(DoubleSchema), typeof(StringSchema) }, TestName = "5")]
    [TestFixture(4096F, 4096D, TypeArgs = new Type[] { typeof(float), typeof(double), typeof(string), typeof(FloatSchema), typeof(DoubleSchema), typeof(StringSchema) }, TestName = "6")]
    [TestFixture("abcd", new byte[] { 0x61, 0x62, 0x63, 0x64 }, TypeArgs = new Type[] { typeof(string), typeof(byte[]), typeof(double), typeof(StringSchema), typeof(BytesSchema), typeof(DoubleSchema) }, TestName = "7")]
    [TestFixture(new byte[] { 0x61, 0x62, 0x63, 0x64 }, "abcd", TypeArgs = new Type[] { typeof(byte[]), typeof(string), typeof(double), typeof(BytesSchema), typeof(StringSchema), typeof(DoubleSchema) }, TestName = "8")]
    public class UnionUpscaleTest<TWriter, TReader1, TReader2, SWriter, SReader1, SReader2> where SWriter : AvroSchema, new() where SReader1 : AvroSchema, new() where SReader2 : AvroSchema, new() where TWriter : notnull where TReader1 : notnull where TReader2 : notnull
    {
        private readonly TWriter _writeValue;
        private readonly AvroUnion<TReader1, TReader2> _readValue;
        private readonly SWriter _writeSchema;
        private readonly UnionSchema _readSchema;
        private readonly DatumWriter<TWriter> _specificWriter;
        private readonly DatumReader<AvroUnion<TReader1, TReader2>> _specificReader;

        public UnionUpscaleTest(TWriter writeValue, TReader1 readValue)
        {
            _writeValue = writeValue;
            _readValue = new AvroUnion<TReader1, TReader2>(readValue);
            _writeSchema = new SWriter();
            _readSchema = new UnionSchema(new SReader1(), new SReader2());
            _specificWriter = new DatumWriter<TWriter>(_writeSchema);
            _specificReader = new DatumReader<AvroUnion<TReader1, TReader2>>(_readSchema, _writeSchema);
        }

        [TestCase]
        public void TestReadWrite()
        {
            using var stream = new MemoryStream();
            using var encoder = new BinaryEncoder(stream);
            using var decoder = new BinaryDecoder(stream);

            _specificWriter.Write(encoder, _writeValue);
            var expectedPosition = stream.Position;
            stream.Seek(0, SeekOrigin.Begin);
            var actualValue = _specificReader.Read(decoder);
            if(typeof(IEnumerable).IsAssignableFrom(_readValue.Type))
                CollectionAssert.AreEqual((IEnumerable)_readValue.Value, (IEnumerable)actualValue.Value);
            else
                Assert.AreEqual(_readValue, actualValue);
            stream.Seek(0, SeekOrigin.Begin);
        }
    }

    [TestFixture("1900-01-01 00:00:00.123", "yyyy-MM-dd HH:mm:ss.fff", "1900-01-01 00:00:00.123000", "yyyy-MM-dd HH:mm:ss.ffffff", TypeArgs = new Type[] { typeof(TimestampMillisSchema), typeof(TimestampMicrosSchema), typeof(DurationSchema) })]
    [TestFixture("1901-01-01 00:00:00.123", "yyyy-MM-dd HH:mm:ss.fff", "1901-01-01 00:00:00.1230000", "yyyy-MM-dd HH:mm:ss.fffffff", TypeArgs = new Type[] { typeof(TimestampMillisSchema), typeof(TimestampNanosSchema), typeof(DurationSchema) })]
    [TestFixture("1901-01-01 00:00:00.123456", "yyyy-MM-dd HH:mm:ss.ffffff", "1901-01-01 00:00:00.1234560", "yyyy-MM-dd HH:mm:ss.fffffff", TypeArgs = new Type[] { typeof(TimestampMicrosSchema), typeof(TimestampNanosSchema), typeof(DurationSchema) })]

    public class UnionUpscaleTimestampTest<SWriter, SReader1, SReader2> : UnionUpscaleTest<DateTime, DateTime, AvroDuration, SWriter, SReader1, SReader2> where SWriter : AvroSchema, new() where SReader1 : AvroSchema, new() where SReader2 : AvroSchema, new()
    {
        public UnionUpscaleTimestampTest(string writeValue, string writeParseString, string readValue, string readParseString)
            : base(DateTime.ParseExact(writeValue, writeParseString, CultureInfo.InvariantCulture), DateTime.ParseExact(readValue, readParseString, CultureInfo.InvariantCulture)) { }
    }

    [TestFixture("12:23:34.123", @"hh\:mm\:ss\.fff", "12:23:34.123000", @"hh\:mm\:ss\.ffffff", TypeArgs = new Type[] { typeof(TimeMillisSchema), typeof(TimeMicrosSchema), typeof(UuidSchema) })]
    [TestFixture("12:23:34.123", @"hh\:mm\:ss\.fff", "12:23:34.1230000", @"hh\:mm\:ss\.fffffff", TypeArgs = new Type[] { typeof(TimeMillisSchema), typeof(TimeNanosSchema), typeof(UuidSchema) })]
    [TestFixture("12:23:34.123456", @"hh\:mm\:ss\.ffffff", "12:23:34.1234560", @"hh\:mm\:ss\.fffffff", TypeArgs = new Type[] { typeof(TimeMicrosSchema), typeof(TimeNanosSchema), typeof(UuidSchema) })]

    public class UnionUpscaleTimeTest<SWriter, SReader1, SReader2> : UnionUpscaleTest<TimeSpan, TimeSpan, Guid, SWriter, SReader1, SReader2> where SWriter : AvroSchema, new() where SReader1 : AvroSchema, new() where SReader2 : AvroSchema, new()
    {
        public UnionUpscaleTimeTest(string writeValue, string writeParseString, string readValue, string readParseString)
            : base(TimeSpan.ParseExact(writeValue, writeParseString, CultureInfo.InvariantCulture), TimeSpan.ParseExact(readValue, readParseString, CultureInfo.InvariantCulture)) { }
    }
}
