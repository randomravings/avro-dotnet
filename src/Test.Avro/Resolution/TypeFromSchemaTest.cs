using Avro;
using Avro.Resolution;
using Avro.Schema;
using Avro.Types;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;

namespace Test.Avro.Resolution
{
    [TestFixture]
    public class TypeFromSchemaTest
    {
        [TestCase(typeof(AvroNull), typeof(NullSchema))]
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
            var assemblies = new[] { Assembly.GetExecutingAssembly() };
            var schemaInstance = (AvroSchema)(Activator.CreateInstance(schema) ?? EmptySchema.Value);
            var actualType = SchemaResolver.GetTypeFromSchema(schemaInstance, assemblies);
            Assert.AreEqual(expectedType, actualType);
        }

        [Test, TestCaseSource(typeof(TypeLookupSource))]
        public void TypeLookupAdvancedTest(Type expectedType, AvroSchema schema)
        {
            var assemblies = new[] { Assembly.GetExecutingAssembly() };
            var actualType = SchemaResolver.GetTypeFromSchema(schema, assemblies);
            Assert.AreEqual(expectedType, actualType);
        }

        [TestCase]
        public void TypeLookupExceptionTest()
        {
            var assemblies = new[] { Assembly.GetExecutingAssembly() };
            var schema = new TypeLookupSchema();
            Assert.Throws<ArgumentException>(() => SchemaResolver.GetTypeFromSchema(schema, assemblies));
        }

        class TypeLookupSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object?[] { typeof(AvroDuration), new DurationSchema() };
                yield return new object?[] { typeof(IList<>).MakeGenericType(typeof(int)), new ArraySchema(new IntSchema()) };
                yield return new object?[] { typeof(IDictionary<,>).MakeGenericType(typeof(string), typeof(Guid)), new MapSchema(new UuidSchema()) };
                yield return new object?[] { typeof(AvroDuration), new DurationSchema() };
                yield return new object?[] { typeof(TypeLookupEnum), new EnumSchema(nameof(TypeLookupEnum), typeof(TypeLookupEnum)?.Namespace ?? string.Empty, new string[] { "A", "B", "C" }) };
                yield return new object?[] { typeof(TypeLookupRecord), new TypeLookupRecord().Schema };
                yield return new object?[] { typeof(TypeLookupError), new TypeLookupError().Schema };
                yield return new object?[] { typeof(TypeLookupFixed), new TypeLookupFixed().Schema };
                yield return new object?[] { typeof(int), new TypeLookupLocialSchema() };
                yield return new object?[] { typeof(GenericEnum), new EnumSchema("SomeUnknownEnum", string.Empty, new string[] { "X", "Y", "Z" }) };
                yield return new object?[] { typeof(Nullable<>).MakeGenericType(typeof(int)), new UnionSchema(new NullSchema(), new IntSchema()) };
                yield return new object?[] { typeof(string), new UnionSchema(new NullSchema(), new StringSchema()) };
                yield return new object?[] { typeof(AvroUnion<AvroNull, string, float>), new UnionSchema(new NullSchema(), new StringSchema(), new FloatSchema()) };
            }
        }
    }

    public class TypeLookupSchema : AvroSchema { }

    public class TypeLookupLocialSchema : LogicalSchema
    {
        public TypeLookupLocialSchema()
            : base(new IntSchema(), "type-lookup-schema") { }
    }

    public enum TypeLookupEnum
    {
        A,
        B,
        C
    }

    public class TypeLookupRecord : IAvroRecord
    {
        public object? this[int i] { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public RecordSchema Schema => AvroParser.ReadSchema<RecordSchema>(@$"{{""name"":""{typeof(TypeLookupRecord).Namespace}.{typeof(TypeLookupRecord).Name}"",""type"":""record"",""fields"":[]}}");

        public int FieldCount => throw new NotImplementedException();
    }

    public class TypeLookupError : IAvroError
    {
        public TypeLookupError() { }

        public object? this[int i] { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public ErrorSchema Schema => AvroParser.ReadSchema<ErrorSchema>(@$"{{""name"":""{typeof(TypeLookupError).Namespace}.{typeof(TypeLookupError).Name}"",""type"":""error"",""fields"":[]}}");

        public int FieldCount => throw new NotImplementedException();
    }

    public class TypeLookupFixed : IAvroFixed
    {
        public FixedSchema Schema => AvroParser.ReadSchema<FixedSchema>(@$"{{""name"":""{typeof(TypeLookupFixed).Namespace}.{typeof(TypeLookupFixed).Name}"",""type"":""fixed"",""size"":12}}");

        public int Size => throw new NotImplementedException();

        public byte[] Value => throw new NotImplementedException();

        public bool Equals(IAvroFixed other) => throw new NotImplementedException();
        public byte this[int i] { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public IEnumerator<byte> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}
