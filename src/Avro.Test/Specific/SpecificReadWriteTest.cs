using Avro.IO;
using Avro.Schemas;
using Avro.Specific;
using NUnit.Framework;
using System;
using System.Globalization;
using System.IO;

namespace Avro.Test.Specific
{
    [TestFixture(1, 1L, TypeArgs = new Type[] { typeof(int), typeof(long), typeof(IntSchema), typeof(LongSchema) })]
    [TestFixture(1, 1F, TypeArgs = new Type[] { typeof(int), typeof(float), typeof(IntSchema), typeof(FloatSchema) })]
    [TestFixture(1, 1D, TypeArgs = new Type[] { typeof(int), typeof(double), typeof(IntSchema), typeof(DoubleSchema) })]
    [TestFixture(1L, 1F, TypeArgs = new Type[] { typeof(long), typeof(float), typeof(LongSchema), typeof(FloatSchema) })]
    [TestFixture(1L, 1D, TypeArgs = new Type[] { typeof(long), typeof(double), typeof(LongSchema), typeof(DoubleSchema) })]
    [TestFixture(1F, 1D, TypeArgs = new Type[] { typeof(float), typeof(double), typeof(FloatSchema), typeof(DoubleSchema) })]
    [TestFixture("abcd", new byte[] { 0x61, 0x62, 0x63, 0x64 }, TypeArgs = new Type[] { typeof(string), typeof(byte[]), typeof(StringSchema), typeof(BytesSchema) })]
    [TestFixture(new byte[] { 0x61, 0x62, 0x63, 0x64 }, "abcd", TypeArgs = new Type[] { typeof(byte[]), typeof(string), typeof(BytesSchema), typeof(StringSchema) })]
    public class SpecificPromoted<TWriter, TReader, SWriter, SReader> where SWriter : Schema, new() where SReader : Schema, new()
    {
        private readonly TWriter _writeValue;
        private readonly TReader _readValue;
        private readonly SWriter _writeSchema;
        private readonly SReader _readSchema;
        private readonly SpecificWriter<TWriter> _specificWriter;
        private readonly SpecificReader<TReader> _specificReader;

        public SpecificPromoted(TWriter writeValue, TReader readValue)
            : this(writeValue, readValue, new SWriter(), new SReader()) { }

        public SpecificPromoted(TWriter writeValue, TReader readValue, object writeInstance, object readInstance)
        {
            _writeValue = writeValue;
            _readValue = readValue;
            _writeSchema = writeInstance as SWriter;
            _readSchema = readInstance as SReader;
            _specificWriter = new SpecificWriter<TWriter>(_writeSchema);
            _specificReader = new SpecificReader<TReader>(_readSchema, _writeSchema);
        }

        [TestCase]
        public void TestSchema()
        {
            Assert.IsTrue(_writeSchema.Equals(_specificWriter.WriterSchema), $"Schema mismatch: {nameof(SpecificWriter<SWriter>)}.{nameof(SpecificWriter<SWriter>.WriterSchema)}");
            Assert.IsTrue(_readSchema.Equals(_specificReader.ReaderSchema), $"Schema mismatch: {nameof(SpecificReader<SReader>)}.{nameof(SpecificReader<SReader>.ReaderSchema)}");
            Assert.IsTrue(_writeSchema.Equals(_specificReader.WriterSchema), $"Schema mismatch: {nameof(SpecificReader<SReader>)}.{nameof(SpecificReader<SReader>.WriterSchema)}");
        }

        [TestCase]
        public void TestReadWrite()
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                _specificWriter.Write(encoder, _writeValue);
                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = _specificReader.Read(decoder);
                Assert.AreEqual(_readValue, actualValue);
                stream.Seek(0, SeekOrigin.Begin);
                actualValue = _specificReader.Read(decoder, ref actualValue);
                Assert.AreEqual(_readValue, actualValue);
                stream.Seek(0, SeekOrigin.Begin);
                _specificReader.Skip(decoder);
                Assert.AreEqual(stream.Position, expectedPosition);
            }
        }
    }

    [TestFixture(null, TypeArgs = new Type[] { typeof(int?), typeof(NullSchema) })]
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
    public class SpecificPrimitiveMirrored<T, S> : SpecificPromoted<T, T, S, S> where S : Schema, new()
    {
        public SpecificPrimitiveMirrored(T value)
            : base(value, value) { }

        public SpecificPrimitiveMirrored(T value, object instance)
            : base(value, value, instance, instance) { }
    }

    [TestFixture("1900-01-01", "yyyy-MM-dd", TypeArgs = new Type[] { typeof(DateSchema) })]
    [TestFixture("2019-03-17", "yyyy-MM-dd", TypeArgs = new Type[] { typeof(DateSchema) })]
    [TestFixture("0001-01-01", "yyyy-MM-dd", TypeArgs = new Type[] { typeof(DateSchema) })]
    [TestFixture("9999-12-31", "yyyy-MM-dd", TypeArgs = new Type[] { typeof(DateSchema) })]
    [TestFixture("1900-01-01 00:00:00.000", "yyyy-MM-dd HH:mm:ss.fff", TypeArgs = new Type[] { typeof(TimestampMillisSchema) })]
    [TestFixture("2031-10-31 23:43:11.873", "yyyy-MM-dd HH:mm:ss.fff", TypeArgs = new Type[] { typeof(TimestampMillisSchema) })]
    [TestFixture("1900-01-01 00:00:00.000000", "yyyy-MM-dd HH:mm:ss.ffffff", TypeArgs = new Type[] { typeof(TimestampMicrosSchema) })]
    [TestFixture("1977-10-14 04:03:59.123456", "yyyy-MM-dd HH:mm:ss.ffffff", TypeArgs = new Type[] { typeof(TimestampMicrosSchema) })]
    [TestFixture("1900-01-01 00:00:00.0000000", "yyyy-MM-dd HH:mm:ss.fffffff", TypeArgs = new Type[] { typeof(TimestampNanosSchema) })]
    [TestFixture("4902-04-01 14:43:32.1234567", "yyyy-MM-dd HH:mm:ss.fffffff", TypeArgs = new Type[] { typeof(TimestampNanosSchema) })]
    public class SpecificDateTimeMirrored<S> : SpecificPrimitiveMirrored<DateTime, S> where S : Schema, new()
    {
        public SpecificDateTimeMirrored(string value, string parseString)
            : base(DateTime.ParseExact(value, parseString, CultureInfo.InvariantCulture)) { }
    }

    [TestFixture("12:34:56.123", @"hh\:mm\:ss\.fff", TypeArgs = new Type[] { typeof(TimeMillisSchema) })]
    [TestFixture("12:34:56.123456", @"hh\:mm\:ss\.ffffff", TypeArgs = new Type[] { typeof(TimeMicrosSchema) })]
    [TestFixture("12:34:56.1234567", @"hh\:mm\:ss\.fffffff", TypeArgs = new Type[] { typeof(TimeNanosSchema) })]
    public class SpecificTimeSpanMirrored<S> : SpecificPrimitiveMirrored<TimeSpan, S> where S : Schema, new()
    {
        public SpecificTimeSpanMirrored(string value, string parseString)
            : base(TimeSpan.ParseExact(value, parseString, CultureInfo.InvariantCulture)) { }
    }


    [TestFixture("0adc0a2a-ea6b-49a8-b767-f54e64e16f2c", TypeArgs = new Type[] { typeof(UuidSchema) })]
    [TestFixture("caa0cc90-84f3-4d0b-8b2a-6ed2a50fdac5", TypeArgs = new Type[] { typeof(UuidSchema) })]
    [TestFixture("fbca64a3-38c4-4553-9fe0-98ebf30fd276", TypeArgs = new Type[] { typeof(UuidSchema) })]
    public class SpecificUuidMirrored<S> : SpecificPrimitiveMirrored<Guid, S> where S : Schema, new()
    {
        public SpecificUuidMirrored(string value)
            : base(Guid.Parse(value)) { }
    }

    [TestFixture(0, 0, 0, TypeArgs = new Type[] { typeof(DurationSchema) })]
    [TestFixture(12, 45, 98234, TypeArgs = new Type[] { typeof(DurationSchema) })]
    [TestFixture(int.MaxValue, int.MaxValue, int.MaxValue, TypeArgs = new Type[] { typeof(DurationSchema) })]
    public class SpecificDurationMirrored<S> : SpecificPrimitiveMirrored<ValueTuple<int, int, int>, S> where S : Schema, new()
    {
        public SpecificDurationMirrored(int mm, int dd, int ms)
            : base(new ValueTuple<int, int, int>(mm, dd, ms)) { }
    }

    [TestFixture("0", 1, 0)]
    [TestFixture("12", 2, 0)]
    [TestFixture("345.543", 15, 6)]
    [TestFixture("-7922816251426433.7593543950335", 29, 13)]
    [TestFixture("7922816251426433.7593543950335", 29, 13)]
    public class SpecificDecimalMirrored : SpecificPrimitiveMirrored<decimal, DecimalSchema>
    {
        public SpecificDecimalMirrored(string value, int precision, int scale)
            : base(decimal.Parse(value), new DecimalSchema(new BytesSchema(), precision, scale)) { }
    }

    [TestFixture("0", 1, 0)]
    [TestFixture("12", 2, 0)]
    [TestFixture("345.543", 15, 6)]
    [TestFixture("-7922816251426433.7593543950335", 28, 13)]
    [TestFixture("7922816251426433.7593543950335", 28, 13)]
    public class SpecificDecimalFixedMirrored : SpecificPrimitiveMirrored<decimal, DecimalSchema>
    {
        public SpecificDecimalFixedMirrored(string value, int precision, int scale)
            : base(decimal.Parse(value), new DecimalSchema(new FixedSchema("decimal", null, 32), precision, scale)) { }
    }

    [TestFixture("2001-03-31 12:34:56.123", "yyyy-MM-dd HH:mm:ss.fff", "2001-03-31 12:34:56.123000", "yyyy-MM-dd HH:mm:ss.ffffff", TypeArgs = new Type[] { typeof(TimestampMillisSchema), typeof(TimestampMicrosSchema) })]
    [TestFixture("2001-03-31 12:34:56.123", "yyyy-MM-dd HH:mm:ss.fff", "2001-03-31 12:34:56.1230000", "yyyy-MM-dd HH:mm:ss.fffffff", TypeArgs = new Type[] { typeof(TimestampMillisSchema), typeof(TimestampNanosSchema) })]
    [TestFixture("2001-03-31 12:34:56.123456", "yyyy-MM-dd HH:mm:ss.ffffff", "2001-03-31 12:34:56.1234560", "yyyy-MM-dd HH:mm:ss.fffffff", TypeArgs = new Type[] { typeof(TimestampMicrosSchema), typeof(TimestampNanosSchema) })]
    public class SpecificDateTimePromoted<SWriter, SReader> : SpecificPromoted<DateTime, DateTime, SWriter, SReader> where SWriter : Schema, new() where SReader : Schema, new()
    {
        public SpecificDateTimePromoted(string writeValue, string writeParseString, string readValue, string readParseString)
            : base(DateTime.ParseExact(writeValue, writeParseString, CultureInfo.InvariantCulture), DateTime.ParseExact(readValue, readParseString, CultureInfo.InvariantCulture)) { }
    }

    [TestFixture("12:34:56.123", @"hh\:mm\:ss\.fff", "12:34:56.123", @"hh\:mm\:ss\.fff", TypeArgs = new Type[] { typeof(TimeMillisSchema), typeof(TimeMicrosSchema) })]
    [TestFixture("12:34:56.123", @"hh\:mm\:ss\.fff", "12:34:56.123000", @"hh\:mm\:ss\.ffffff", TypeArgs = new Type[] { typeof(TimeMillisSchema), typeof(TimeNanosSchema) })]
    [TestFixture("12:34:56.123456", @"hh\:mm\:ss\.ffffff", "12:34:56.1234560", @"hh\:mm\:ss\.fffffff", TypeArgs = new Type[] { typeof(TimeMicrosSchema), typeof(TimeNanosSchema) })]
    public class SpecificTimeSpanPromoted<SWriter, SReader> : SpecificPromoted<TimeSpan, TimeSpan, SWriter, SReader> where SWriter : Schema, new() where SReader : Schema, new()
    {
        public SpecificTimeSpanPromoted(string writeValue, string writeParseString, string readValue, string readParseString)
            : base(TimeSpan.ParseExact(writeValue, writeParseString, CultureInfo.InvariantCulture), TimeSpan.ParseExact(readValue, readParseString, CultureInfo.InvariantCulture)) { }
    }
}
