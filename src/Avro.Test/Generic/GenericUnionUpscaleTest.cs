﻿using Avro.Generic;
using Avro.IO;
using Avro.Schemas;
using NUnit.Framework;
using System;
using System.Globalization;
using System.IO;

namespace Avro.Test.Generic
{
    [TestFixture(123, 123L, TypeArgs = new Type[] { typeof(int), typeof(IntSchema), typeof(LongSchema), typeof(FloatSchema)})]
    [TestFixture(456, 456F, TypeArgs = new Type[] { typeof(int), typeof(IntSchema), typeof(FloatSchema), typeof(DoubleSchema) })]
    [TestFixture(789, 789D, TypeArgs = new Type[] { typeof(int), typeof(IntSchema), typeof(DoubleSchema), typeof(StringSchema) })]
    [TestFixture(1024L, 1024F, TypeArgs = new Type[] { typeof(long), typeof(LongSchema), typeof(FloatSchema), typeof(DoubleSchema) })]
    [TestFixture(2048L, 2048D, TypeArgs = new Type[] { typeof(long), typeof(LongSchema), typeof(DoubleSchema), typeof(StringSchema) })]
    [TestFixture(4096F, 4096D, TypeArgs = new Type[] { typeof(float), typeof(FloatSchema), typeof(DoubleSchema), typeof(StringSchema) })]
    [TestFixture("abcd", new byte[] { 0x61, 0x62, 0x63, 0x64 }, TypeArgs = new Type[] { typeof(string), typeof(StringSchema), typeof(BytesSchema), typeof(DoubleSchema) })]
    [TestFixture(new byte[] { 0x61, 0x62, 0x63, 0x64 }, "abcd", TypeArgs = new Type[] { typeof(byte[]), typeof(BytesSchema), typeof(StringSchema), typeof(DoubleSchema) })]
    public class GenericUnionUpscaleTest<TWriter, SWriter, WUnionSchemaOne, WUnionSchemaTwo> where SWriter : Schema, new() where WUnionSchemaOne : Schema, new() where WUnionSchemaTwo : Schema, new()
    {
        private readonly TWriter _writeValue;
        private readonly object _readValue;
        private readonly SWriter _writeSchema;
        private readonly UnionSchema _readSchema;
        private readonly GenericWriter<TWriter> _genericWriter;
        private readonly GenericReader<object> _genericReader;

        public GenericUnionUpscaleTest(TWriter writeValue, object readValue)
        {
            _writeValue = writeValue;
            _readValue = readValue;
            _writeSchema = new SWriter();
            _readSchema = new UnionSchema(new WUnionSchemaOne(), new WUnionSchemaTwo());
            _genericWriter = new GenericWriter<TWriter>(_writeSchema);
            _genericReader = new GenericReader<object>(_readSchema, _writeSchema);
        }

        [TestCase]
        public void TestReadWrite()
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                _genericWriter.Write(encoder, _writeValue);
                var expectedPosition = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                var actualValue = _genericReader.Read(decoder);
                Assert.AreEqual(_readValue, actualValue);
                stream.Seek(0, SeekOrigin.Begin);
                actualValue = _genericReader.Read(decoder, ref actualValue);
                Assert.AreEqual(_readValue, actualValue);
                stream.Seek(0, SeekOrigin.Begin);
                _genericReader.Skip(decoder);
                Assert.AreEqual(stream.Position, expectedPosition);
            }
        }
    }

    [TestFixture("1900-01-01 00:00:00.123", "yyyy-MM-dd HH:mm:ss.fff", "1900-01-01 00:00:00.123000", "yyyy-MM-dd HH:mm:ss.ffffff", TypeArgs = new Type[] { typeof(TimestampMillisSchema), typeof(TimestampMicrosSchema), typeof(TimestampNanosSchema) })]
    [TestFixture("1901-01-01 00:00:00.123", "yyyy-MM-dd HH:mm:ss.fff", "1901-01-01 00:00:00.1230000", "yyyy-MM-dd HH:mm:ss.fffffff", TypeArgs = new Type[] { typeof(TimestampMillisSchema), typeof(TimestampNanosSchema), typeof(DurationSchema) })]
    [TestFixture("1901-01-01 00:00:00.123456", "yyyy-MM-dd HH:mm:ss.ffffff", "1901-01-01 00:00:00.1234560", "yyyy-MM-dd HH:mm:ss.fffffff", TypeArgs = new Type[] { typeof(TimestampMicrosSchema), typeof(TimestampNanosSchema), typeof(DurationSchema) })]
    public class GenericUnionUpscaleTimestampTest<SWriter, WUnionSchemaOne, WUnionSchemaTwo> : GenericUnionUpscaleTest<DateTime, SWriter, WUnionSchemaOne, WUnionSchemaTwo> where SWriter : Schema, new() where WUnionSchemaOne : Schema, new() where WUnionSchemaTwo : Schema, new()
    {
        public GenericUnionUpscaleTimestampTest(string writeValue, string writeParseString, string readValue, string readParseString)
            : base(DateTime.ParseExact(writeValue, writeParseString, CultureInfo.InvariantCulture), DateTime.ParseExact(readValue, readParseString, CultureInfo.InvariantCulture)) { }
    }

    [TestFixture("12:23:34.123", @"hh\:mm\:ss\.fff", "12:23:34.123000", @"hh\:mm\:ss\.ffffff", TypeArgs = new Type[] { typeof(TimeMillisSchema), typeof(TimeMicrosSchema), typeof(TimeNanosSchema) })]
    [TestFixture("12:23:34.123", @"hh\:mm\:ss\.fff", "12:23:34.1230000", @"hh\:mm\:ss\.fffffff", TypeArgs = new Type[] { typeof(TimeMillisSchema), typeof(TimeNanosSchema), typeof(UuidSchema) })]
    [TestFixture("12:23:34.123456", @"hh\:mm\:ss\.ffffff", "12:23:34.1234560", @"hh\:mm\:ss\.fffffff", TypeArgs = new Type[] { typeof(TimeMicrosSchema), typeof(TimeNanosSchema), typeof(UuidSchema) })]
    public class GenericUnionUpscaleTimeTest<SWriter, WUnionSchemaOne, WUnionSchemaTwo> : GenericUnionUpscaleTest<TimeSpan, SWriter, WUnionSchemaOne, WUnionSchemaTwo> where SWriter : Schema, new() where WUnionSchemaOne : Schema, new() where WUnionSchemaTwo : Schema, new()
    {
        public GenericUnionUpscaleTimeTest(string writeValue, string writeParseString, string readValue, string readParseString)
            : base(TimeSpan.ParseExact(writeValue, writeParseString, CultureInfo.InvariantCulture), TimeSpan.ParseExact(readValue, readParseString, CultureInfo.InvariantCulture)) { }
    }
}
