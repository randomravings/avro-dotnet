using Avro;
using Avro.Schema;
using Avro.Utils;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Test.Avro.Schema
{
    [TestFixture]
    public class SchemaDefaultTest
    {
        [Test, TestCaseSource(typeof(SchemaDefaults))]
        public void TestDefault(AvroSchema schema, string defaultValue)
        {
            Assert.DoesNotThrow(() => DefaultValidator.ValidateString(schema, defaultValue));
        }

        class SchemaDefaults : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new NullSchema(), "null" };
                yield return new object[] { new BooleanSchema(), "true" };
                yield return new object[] { new BooleanSchema(), "false" };
                yield return new object[] { new IntSchema(), "123" };
                yield return new object[] { new LongSchema(), "456" };
                yield return new object[] { new FloatSchema(), "789123" };
                yield return new object[] { new FloatSchema(), "789.123" };
                yield return new object[] { new DoubleSchema(), "789123" };
                yield return new object[] { new DoubleSchema(), "789.123" };
                yield return new object[] { new BytesSchema(), @"""\\u0000\\u0012\\u00FF""" };
                yield return new object[] { new StringSchema(), @"""Stuff123/%¤""" };
                yield return new object[] { new ArraySchema(new IntSchema()), @"[]" };
                yield return new object[] { new ArraySchema(new IntSchema()), @"[1,2,3,4]" };
                yield return new object[] { new ArraySchema(new StringSchema()), @"[""1"",""2"",""3"",""4""]" };
                yield return new object[] { new MapSchema(new IntSchema()), @"{}" };
                yield return new object[] { new MapSchema(new IntSchema()), @"{""one"":1,""two"":2,""three"":3,""four"":4}" };
                yield return new object[] { new MapSchema(new StringSchema()), @"{""one"":""!"",""two"":""%"",""three"":""Foo"",""four"":""bAR""}" };
                yield return new object[] { new EnumSchema("X", string.Empty, new string[] { "A", "B", "C" }), @"""C""" };
                yield return new object[] { new FixedSchema("X", string.Empty, 4), @"""\\u0000\\u0067\\u00A9\\u00FF""" };
                yield return new object[] { new UuidSchema(), @"""ad210816-e1c0-4fdc-87e5-96262229a70a""" };
                yield return new object[] { new UuidSchema(), @"""AD210816-E1C0-4FDC-87E5-96262229A70A""" };
                yield return new object[] { new UnionSchema(new NullSchema(), new IntSchema()), @"null" };
                yield return new object[] { new UnionSchema(new LongSchema(), new StringSchema()), @"87" };
                yield return new object[] { new RecordSchema("X", string.Empty,
                                                new RecordFieldSchema[]
                                                {
                                                    new RecordFieldSchema("f1", new IntSchema()),
                                                    new RecordFieldSchema("f2", new StringSchema()) { Default = @"""ABC""" }
                                                }
                                            ),
                                            @"{ ""f1"": 123 }"
                };
                yield return new object[] { new DateSchema(), @"123" };
            }
        }

        class SchemaErrorDefaults : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new NullSchema(), "0" };
                yield return new object[] { new BooleanSchema(), "1" };
                yield return new object[] { new IntSchema(), @"""x""" };
                yield return new object[] { new LongSchema(), "{}" };
                yield return new object[] { new FloatSchema(), @"""string""" };
                yield return new object[] { new DoubleSchema(), @"""\\u00FF""" };
                yield return new object[] { new BytesSchema(), @"[0, 255]" };
                yield return new object[] { new BytesSchema(), @"""\\uFFFF""" };
                yield return new object[] { new StringSchema(), @"123" };
                yield return new object[] { new ArraySchema(new IntSchema()), @"1" };
                yield return new object[] { new ArraySchema(new IntSchema()), @"[""1""]" };
                yield return new object[] { new MapSchema(new StringSchema()), @"""one""" };
                yield return new object[] { new MapSchema(new StringSchema()), @"{""one"":1}" };
                yield return new object[] { new EnumSchema("X", string.Empty, new string[] { "A", "B", "C" }), @"22" };
                yield return new object[] { new EnumSchema("X", string.Empty, new string[] { "A", "B", "C" }), @"""X""" };
                yield return new object[] { new FixedSchema("X", string.Empty, 3), @"123" };
                yield return new object[] { new FixedSchema("X", string.Empty, 3), @"""\\u0000\\u0067\\u00A9\\u00FF""" };
                yield return new object[] { new UuidSchema(), @"123" };
                yield return new object[] { new UuidSchema(), @"""ad210816""" };
                yield return new object[] { new UnionSchema(new NullSchema(), new IntSchema()), @"0" };
                yield return new object[] { new UnionSchema(new LongSchema(), new StringSchema()), @"""87""" };
                yield return new object[] { new RecordSchema("X", string.Empty,
                                                new RecordFieldSchema[]
                                                {
                                                    new RecordFieldSchema("f1", new IntSchema()),
                                                    new RecordFieldSchema("f2", new StringSchema())
                                                }
                                            ),
                                            @"{ ""f1"": 123 }"
                };
                yield return new object[] { new RecordSchema("X", string.Empty,
                                                new RecordFieldSchema[]
                                                {
                                                    new RecordFieldSchema("f1", new IntSchema()),
                                                    new RecordFieldSchema("f2", new StringSchema())
                                                }
                                            ),
                                            @"123"
                };
                yield return new object[] { new RecordSchema("X", string.Empty,
                                                new RecordFieldSchema[]
                                                {
                                                    new RecordFieldSchema("f1", new IntSchema()),
                                                    new RecordFieldSchema("f2", new StringSchema()) { Default = @"""ABC""" }
                                                }
                                            ),
                                            @"{ ""f1"": ""123"" }"
                };
                yield return new object[] { new RecordSchema("X", string.Empty,
                                                new RecordFieldSchema[]
                                                {
                                                    new RecordFieldSchema("f2", new StringSchema()) { Default = @"""ABC""" }
                                                }
                                            ),
                                            @"{ ""f3"": ""456"" }"
                };
                yield return new object[] { new DateSchema(), @"""2012-04-23T18:25:43.511Z""" };
                yield return new object[] { new UnknowSchema(), @"123" };
            }
        }

        class UnknowSchema : AvroSchema { }
    }
}
