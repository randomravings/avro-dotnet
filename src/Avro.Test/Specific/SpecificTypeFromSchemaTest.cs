using Avro.IO;
using Avro.Schemas;
using Avro.Specific;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;

namespace Avro.Test.Specific
{
    [TestFixture]
    public class SpecificTypeFromSchemaTest
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
            var assembly = Assembly.GetExecutingAssembly();
            var schemaInstance = Activator.CreateInstance(schema) as Schema;
            var actualType = SpecificResolver.GetTypeFromSchema(schemaInstance, assembly);
            Assert.AreEqual(expectedType, actualType);
        }

        [Test, TestCaseSource(typeof(TypeLookupSource))]
        public void TypeLookupAdvancedTest(Type expectedType, Schema schema)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var actualType = SpecificResolver.GetTypeFromSchema(schema, assembly);
            Assert.AreEqual(expectedType, actualType);
        }

        [TestCase]
        public void TypeLookupExceptionTest()
        {
            var assembly = Assembly.GetExecutingAssembly();
            var schema = new TypeLookupSchema();
            Assert.Throws<ArgumentException>(() => SpecificResolver.GetTypeFromSchema(schema, assembly));
        }

        class TypeLookupSource : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { typeof(ValueTuple<,,>).MakeGenericType(typeof(uint), typeof(uint), typeof(uint)), new DurationSchema() };
                yield return new object[] { typeof(IList<>).MakeGenericType(typeof(int)), new ArraySchema(new IntSchema()) };
                yield return new object[] { typeof(IDictionary<,>).MakeGenericType(typeof(string), typeof(Guid)), new MapSchema(new UuidSchema()) };
                yield return new object[] { typeof(ValueTuple<,,>).MakeGenericType(typeof(uint), typeof(uint), typeof(uint)), new DurationSchema() };
                yield return new object[] { typeof(TypeLookupEnum), new EnumSchema(nameof(TypeLookupEnum), typeof(TypeLookupEnum).Namespace, new string[] { "A", "B", "C" }) };
                yield return new object[] { typeof(TypeLookupRecord), new TypeLookupRecord().Schema };
                yield return new object[] { typeof(TypeLookupError), new TypeLookupError().Schema };
                yield return new object[] { typeof(TypeLookupFixed), new TypeLookupFixed().Schema };
                yield return new object[] { typeof(int), new TypeLookupLocialSchema() };
                yield return new object[] { null, new EnumSchema("SomeUnknownEnum", null, new string[] { "X", "Y", "Z" }) };
                yield return new object[] { typeof(Nullable<>).MakeGenericType(typeof(int)), new UnionSchema(new NullSchema(), new IntSchema()) };
                yield return new object[] { typeof(string), new UnionSchema(new NullSchema(), new StringSchema()) };
                yield return new object[] { typeof(object), new UnionSchema(new NullSchema(), new StringSchema(), new FloatSchema()) };
            }
        }
    }

    public class TypeLookupSchema : Schema {}

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

    public class TypeLookupRecord : ISpecificRecord
    {
        public object this[int i] { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public Schema Schema => Schema.Parse(@"{""name"":""Avro.Test.Specific.TypeLookupRecord"",""type"":""record"",""fields"":[]}");

        public int FieldCount => throw new NotImplementedException();

        public object Get(int fieldPos)
        {
            throw new NotImplementedException();
        }

        public void Put(int fieldPos, object fieldValue)
        {
            throw new NotImplementedException();
        }
    }

    public class TypeLookupError : SpecificError, ISpecificRecord
    {
        public TypeLookupError()
            : base(string.Empty) { }

        public object this[int i] { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public Schema Schema => Schema.Parse(@"{""name"":""Avro.Test.Specific.TypeLookupError"",""type"":""error"",""fields"":[]}");

        public int FieldCount => throw new NotImplementedException();

        public object Get(int fieldPos)
        {
            throw new NotImplementedException();
        }

        public void Put(int fieldPos, object fieldValue)
        {
            throw new NotImplementedException();
        }
    }

    public class TypeLookupFixed : ISpecificFixed
    {
        public Schema Schema => Schema.Parse(@"{""name"":""Avro.Test.Specific.TypeLookupFixed"",""type"":""fixed"",""size"":12}");

        public int Size => throw new NotImplementedException();

        public byte[] Value => throw new NotImplementedException();

        public bool Equals(ISpecificFixed other) => throw new NotImplementedException();
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
