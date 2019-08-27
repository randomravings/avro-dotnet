using Avro.Schema;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Avro.Test.Schema
{
    [TestFixture()]
    public class SchemaConstructTest
    {
        [Test, TestCaseSource(typeof(NoExeptionStructors))]
        public void SchemaConstructOK(Func<AvroSchema> invoker)
        {
            Assert.DoesNotThrow(() => invoker.Invoke());
        }

        [Test, TestCaseSource(typeof(ExeptionStructors))]
        public void SchemaConstructError(Action invoker, Type exceptionType)
        {
            Assert.Throws(exceptionType, () => invoker.Invoke());
        }

        class NoExeptionStructors : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new Func<AvroSchema>(() => new DateSchema());
                yield return new Func<AvroSchema>(() => new DateSchema(new IntSchema()));

                yield return new Func<AvroSchema>(() => new TimeMillisSchema());
                yield return new Func<AvroSchema>(() => new TimeMillisSchema(new IntSchema()));
                yield return new Func<AvroSchema>(() => new TimeMicrosSchema());
                yield return new Func<AvroSchema>(() => new TimeMicrosSchema(new LongSchema()));
                yield return new Func<AvroSchema>(() => new TimeNanosSchema());
                yield return new Func<AvroSchema>(() => new TimeNanosSchema(new LongSchema()));

                yield return new Func<AvroSchema>(() => new TimestampMillisSchema());
                yield return new Func<AvroSchema>(() => new TimestampMillisSchema(new LongSchema()));
                yield return new Func<AvroSchema>(() => new TimestampMicrosSchema());
                yield return new Func<AvroSchema>(() => new TimestampMicrosSchema(new LongSchema()));
                yield return new Func<AvroSchema>(() => new TimestampNanosSchema());
                yield return new Func<AvroSchema>(() => new TimestampNanosSchema(new LongSchema()));

                yield return new Func<AvroSchema>(() => new DecimalSchema(new BytesSchema()));
                yield return new Func<AvroSchema>(() => new DecimalSchema(20, 8));

                yield return new Func<AvroSchema>(() => new RecordSchema());
                yield return new Func<AvroSchema>(() => new RecordSchema("TestRecordSchema"));
                yield return new Func<AvroSchema>(() => new RecordSchema("TestRecordSchema", "TestNamespace"));
                yield return new Func<AvroSchema>(() => new RecordSchema("TestRecordSchema", new List<RecordSchema.Field>() { new RecordSchema.Field("X", new IntSchema()) }));
                yield return new Func<AvroSchema>(() => new RecordSchema("TestRecordSchema", "TestNamespace", new List<RecordSchema.Field>() { new RecordSchema.Field("TestField") }));

                yield return new Func<AvroSchema>(() => new ErrorSchema());
                yield return new Func<AvroSchema>(() => new ErrorSchema("TestErrorSchema"));
                yield return new Func<AvroSchema>(() => new ErrorSchema("TestErrorSchema", "TestNamespace"));
                yield return new Func<AvroSchema>(() => new ErrorSchema("TestErrorSchema", new List<RecordSchema.Field>()));
                yield return new Func<AvroSchema>(() => new ErrorSchema("TestErrorSchema", "TestNamespace", new List<RecordSchema.Field>() { new RecordSchema.Field("TestField") }));
            }
        }

        class ExeptionStructors : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new Action(() => new DecimalSchema(new ArraySchema(new IntSchema()))), typeof(AvroParseException) };
                yield return new object[] { new Action(() => new DecimalSchema(20, -1)), typeof(AvroParseException) };
                yield return new object[] { new Action(() => new DecimalSchema(4, 8)), typeof(AvroParseException) };
                yield return new object[] { new Action(() => new DecimalSchema(0, 0)), typeof(AvroParseException) };

                yield return new object[] { new Action(() => new DurationSchema(new IntSchema())), typeof(AvroParseException) };
                yield return new object[] { new Action(() => new DurationSchema(new FixedSchema("WrongSize", null, 4))), typeof(AvroParseException) };

                yield return new object[] { new Action(() => new FixedSchema("TestFixed", "TestNamespace", -1)), typeof(AvroParseException) };

                yield return new object[] { new Action(() => new UuidSchema(new FloatSchema())), typeof(AvroParseException) };

                yield return new object[] { new Action(() => new DateSchema(new StringSchema())), typeof(AvroParseException) };

                yield return new object[] { new Action(() => new TimeMillisSchema(new NullSchema())), typeof(AvroParseException) };
                yield return new object[] { new Action(() => new TimeMicrosSchema(new BooleanSchema())), typeof(AvroParseException) };
                yield return new object[] { new Action(() => new TimeNanosSchema(new IntSchema())), typeof(AvroParseException) };

                yield return new object[] { new Action(() => new TimestampMillisSchema(new NullSchema())), typeof(AvroParseException) };
                yield return new object[] { new Action(() => new TimestampMicrosSchema(new BooleanSchema())), typeof(AvroParseException) };
                yield return new object[] { new Action(() => new TimestampNanosSchema(new IntSchema())), typeof(AvroParseException) };
            }
        }
    }
}
