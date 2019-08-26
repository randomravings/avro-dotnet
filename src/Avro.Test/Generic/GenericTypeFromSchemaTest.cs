using Avro.Generic;
using Avro.Schemas;
using Avro.Types;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Avro.Test.Generic
{
    [TestFixture]
    public class GenericTypeFromSchemaTest
    {
        [TestCase(typeof(object), typeof(NullSchema))]
        [TestCase(typeof(int), typeof(IntSchema))]
        [TestCase(typeof(long), typeof(LongSchema))]
        [TestCase(typeof(float), typeof(FloatSchema))]
        [TestCase(typeof(double), typeof(DoubleSchema))]
        [TestCase(typeof(byte[]), typeof(BytesSchema))]
        [TestCase(typeof(string), typeof(StringSchema))]
        [TestCase(typeof(decimal), typeof(DecimalSchema))]
        [TestCase(typeof(DateTime), typeof(DateSchema))]
        [TestCase(typeof(DateTime), typeof(TimestampMillisSchema))]
        [TestCase(typeof(DateTime), typeof(TimestampMicrosSchema))]
        [TestCase(typeof(DateTime), typeof(TimestampNanosSchema))]
        [TestCase(typeof(TimeSpan), typeof(TimeMillisSchema))]
        [TestCase(typeof(TimeSpan), typeof(TimeMicrosSchema))]
        [TestCase(typeof(TimeSpan), typeof(TimeNanosSchema))]
        [TestCase(typeof(Guid), typeof(UuidSchema))]
        public void TypeLookupTest(Type expectedType, Type schema)
        {
            var schemaInstance = Activator.CreateInstance(schema) as AvroSchema;
            var actualType = GenericResolver.GetTypeFromSchema(schemaInstance);
            Assert.AreEqual(expectedType, actualType);
        }

        [Test, TestCaseSource(typeof(TypeLookupSource))]
        public void TypeLookupAdvancedTest(Type expectedType, AvroSchema schema)
        {
            var actualType = GenericResolver.GetTypeFromSchema(schema);
            Assert.AreEqual(expectedType, actualType);
        }

        [TestCase]
        public void TypeLookupExceptionTest()
        {
            var schema = new TypeLookupSchema();
            Assert.Throws<ArgumentException>(() => GenericResolver.GetTypeFromSchema(schema));
        }

        class TypeLookupSource : IEnumerable
        {
            private readonly EnumSchema _typeLookupEnumSchema = AvroParser.ReadSchema(@"{""name"":""Avro.Test.Generic.TypeLookupEnum"",""type"":""enum"",""symbols"":[]}") as EnumSchema;
            private readonly RecordSchema _typeLookupRecordSchema = AvroParser.ReadSchema(@"{""name"":""Avro.Test.Generic.TypeLookupRecord"",""type"":""record"",""fields"":[]}") as RecordSchema;
            private readonly ErrorSchema _typeLookupErrorSchema = AvroParser.ReadSchema(@"{""name"":""Avro.Test.Generic.TypeLookupError"",""type"":""error"",""fields"":[]}") as ErrorSchema;
            private readonly FixedSchema _typeLookupFixedSchema = AvroParser.ReadSchema(@"{""name"":""Avro.Test.Generic.TypeLookupFixed"",""type"":""fixed"",""size"":12}") as FixedSchema;

            public IEnumerator GetEnumerator()
            {
                yield return new object[] { typeof(AvroDuration), new DurationSchema() };
                yield return new object[] { typeof(IList<>).MakeGenericType(typeof(object)), new ArraySchema(new IntSchema()) };
                yield return new object[] { typeof(IDictionary<,>).MakeGenericType(typeof(string), typeof(object)), new MapSchema(new UuidSchema()) };
                yield return new object[] { typeof(AvroDuration), new DurationSchema() };
                yield return new object[] { typeof(GenericAvroEnum), _typeLookupEnumSchema };
                yield return new object[] { typeof(GenericAvroRecord), _typeLookupRecordSchema };
                yield return new object[] { typeof(GenericAvroRecord), _typeLookupErrorSchema };
                yield return new object[] { typeof(GenericAvroFixed), _typeLookupFixedSchema };
                yield return new object[] { typeof(int), new TypeLookupLocialSchema() };
                yield return new object[] { typeof(Nullable<>).MakeGenericType(typeof(int)), new UnionSchema(new NullSchema(), new IntSchema()) };
                yield return new object[] { typeof(string), new UnionSchema(new NullSchema(), new StringSchema()) };
                yield return new object[] { typeof(object), new UnionSchema(new NullSchema(), new StringSchema(), new FloatSchema()) };
            }
        }
    }

    public class TypeLookupSchema : AvroSchema {}

    public class TypeLookupLocialSchema : LogicalSchema
    {
        public TypeLookupLocialSchema()
            : base(new IntSchema(), "type-lookup-schema") { }
    }
}
