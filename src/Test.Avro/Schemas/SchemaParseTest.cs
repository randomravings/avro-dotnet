using Avro;
using Avro.Schema;
using NUnit.Framework;
using System;

namespace Test.Avro.Schema
{
    [TestFixture]
    public class SchemaParseTest
    {
        [TestCase(
            typeof(NullSchema),
            @"""null""",
            @"null",
            TestName = "Null - String"
        )]
        [TestCase(
            typeof(NullSchema),
            @"{""type"": ""null""}",
            @"null",
            TestName = "Null - Object"
        )]
        [TestCase(
            typeof(BooleanSchema),
            @"""boolean""",
            @"boolean",
            TestName = "Boolean - String"
        )]
        [TestCase(
            typeof(BooleanSchema),
            @"{""type"": ""boolean""}",
            @"boolean",
            TestName = "Boolean - Object"
        )]
        [TestCase(
            typeof(IntSchema),
            @"""int""",
            @"int",
            TestName = "Int - String"
        )]
        [TestCase(
            typeof(IntSchema),
            @"{""type"": ""int""}",
            @"int",
            TestName = "Int - Object"
        )]
        [TestCase(
            typeof(LongSchema),
            @"""long""",
            @"long",
            TestName = "Long - String"
        )]
        [TestCase(
            typeof(LongSchema),
            @"{""type"": ""long""}",
            @"long",
            TestName = "Long - Object"
        )]
        [TestCase(
            typeof(FloatSchema),
            @"""float""",
            @"float",
            TestName = "Float - String"
        )]
        [TestCase(
            typeof(FloatSchema),
            @"{""type"": ""float""}",
            @"float",
            TestName = "Float - Object"
        )]
        [TestCase(
            typeof(DoubleSchema),
            @"""double""",
            @"double",
            TestName = "Double - String"
        )]
        [TestCase(
            typeof(DoubleSchema),
            @"{""type"": ""double""}",
            @"double",
            TestName = "Double - Object"
        )]
        [TestCase(
            typeof(BytesSchema),
            @"""bytes""",
            @"bytes",
            TestName = "Bytes - String"
        )]
        [TestCase(
            typeof(BytesSchema),
            @"{""type"": ""bytes""}",
            @"bytes",
            TestName = "Bytes - Object"
        )]
        [TestCase(
            typeof(StringSchema),
            @"""string""",
            @"string",
            TestName = "String - String"
        )]
        [TestCase(
            typeof(StringSchema),
            @"{""type"": ""string""}",
            @"string",
            TestName = "String - Object"
        )]
        [TestCase(
            typeof(ArraySchema),
            @"{""type"": ""array"", ""items"": ""long""}",
            @"array",
            TestName = "Array - Primitive"
        )]
        [TestCase(
            typeof(ArraySchema),
            @"{""type"": ""array"",""items"": {""type"": ""enum"", ""name"": ""Test"", ""symbols"": [""A"", ""B""]}}",
            @"array",
            TestName = "Array - Complex"
        )]
        [TestCase(
            typeof(MapSchema),
            @"{""type"": ""map"", ""values"": ""long""}",
            @"map",
            TestName = "Map - Primitive"
        )]
        [TestCase(
            typeof(MapSchema),
            @"{""type"": ""map"",""values"": {""type"": ""enum"", ""name"": ""Test"", ""symbols"": [""A"", ""B""]}}",
            @"map",
            TestName = "Map - Complex"
        )]
        [TestCase(
            typeof(RecordSchema),
            @"{""type"": ""record"",""name"": ""Test"",""fields"": [{""name"": ""f"",""type"": ""long""}]}",
            @"Test",
            TestName = "Record - Single Field"
        )]
        [TestCase(
            typeof(RecordSchema),
            @"{""type"": ""record"",""name"": ""Test"",""fields"": [{""name"": ""f"",""type"": ""long"",""doc"":""Test Field Documentation"",""aliases"":[""V""],""default"":-1,""order"":""order""}]}",
            @"Test",
            TestName = "Record - Single Field"
        )]
        [TestCase(
            typeof(RecordSchema),
            @"{""type"": ""record"",""name"": ""Test"",""fields"": [{""name"": ""f1"",""type"": ""long""},{""name"": ""f2"", ""type"": ""int""}]}",
            @"Test",
            TestName = "Record - Multi Field"
        )]
        [TestCase(
            typeof(RecordSchema),
            @"{""type"": ""record"",""name"":""LongList"",""fields"":[{""name"":""value"",""type"":""long""},{""name"":""next"",""type"":[""null"", ""LongList""]}]}",
            @"LongList",
            TestName = "Record - Recursive"
        )]
        [TestCase(
            typeof(RecordSchema),
            @"{""type"": ""record"",""name"":""LongList"",""aliases"":[""A"",""B"",""C""], ""doc"": ""Some documentation text"", ""fields"": [{""name"": ""f"",""type"": ""long""}]}",
            @"LongList",
            TestName = "Record - Detailed"
        )]
        [TestCase(
            typeof(ErrorSchema),
            @"{""type"": ""error"",""name"": ""Test"",""fields"": [{""name"": ""f1"",""type"": ""long""},{""name"": ""f2"", ""type"": ""int""}]}",
            @"Test",
            TestName = "Error - Multi Field"
        )]
        [TestCase(
            typeof(EnumSchema),
            @"{""type"": ""enum"", ""name"": ""Test"", ""symbols"": [""A"", ""B""]}",
            @"Test",
            TestName = "Enum - Basic"
        )]
        [TestCase(
            typeof(EnumSchema),
            @"{""type"": ""enum"", ""name"": ""Test"", ""namespace"":""x.y.z"", ""aliases"":[""X"",""Y""], ""doc"":""Test Documentation"", ""symbols"": [""A"", ""B""]}",
            @"Test",
            TestName = "Enum - Detailed"
        )]
        [TestCase(
            typeof(EnumSchema),
            @"{""type"": ""enum"", ""name"": ""Test"", ""symbols"": [""A"", ""B""]}",
            @"Test",
            TestName = "Enum - Aliased"
        )]
        [TestCase(
            typeof(UnionSchema),
            @"[""string""]",
            @"union",
            TestName = "Union - One Type"
        )]
        [TestCase(
            typeof(UnionSchema),
            @"[""string"", ""null"", ""long""]",
            @"union",
            TestName = "Union - Primitives"
        )]
        [TestCase(
            typeof(UnionSchema),
            @"{""type"":[""string"", ""null"", ""long""]}",
            @"union",
            TestName = "Union - As Object"
        )]
        [TestCase(
            typeof(UnionSchema),
            @"[{""type"": ""record"",""name"": ""Test1"",""namespace"":""ns1"",""fields"": [{""name"": ""f"",""type"": ""long""}]},{""type"": ""record"",""name"": ""Test2"",""namespace"":""ns2"",""fields"": [{""name"": ""f"",""type"": ""long""}]}]",
            @"union",
            TestName = "Union - Of Records"
        )]
        [TestCase(
            typeof(FixedSchema),
            @"{""type"": ""fixed"", ""name"": ""Test"", ""size"": 1}",
            @"Test",
            TestName = "Fixed - Named"
        )]
        [TestCase(
            typeof(FixedSchema),
            @"{""type"": ""fixed"", ""name"": ""MyFixed"", ""namespace"": ""org.apache.hadoop.avro"", ""size"": 1}",
            @"MyFixed",
            TestName = "Fixed - Namespaced"
        )]
        [TestCase(
            typeof(DecimalSchema),
            @"{""logicalType"": ""decimal"", ""type"": ""bytes"", ""precision"": 9, ""scale"": 5}",
            @"decimal",
            TestName = "Decimal - As Bytes"
        )]
        [TestCase(
            typeof(DecimalSchema),
            @"{""logicalType"": ""decimal"", ""type"": {""type"": ""fixed"", ""name"": ""decimal"", ""size"": 16}, ""precision"": 9, ""scale"": 5}",
            @"decimal",
            TestName = "Decimal - As Fixed"
        )]
        [TestCase(
            typeof(TimeMillisSchema),
            @"{""logicalType"": ""time-millis"", ""type"": ""int""}",
            "time-millis",
            TestName = "Time - Millisecond"
        )]
        [TestCase(
            typeof(TimeMicrosSchema),
            @"{""logicalType"": ""time-micros"", ""type"": ""long""}",
            @"time-micros",
            TestName = "Time - Microsecond"
        )]
        [TestCase(
            typeof(TimeNanosSchema),
            @"{""logicalType"": ""time-nanos"", ""type"": ""long""}",
            @"time-nanos",
            TestName = "Time - Nanosecond"
        )]
        [TestCase(
            typeof(TimestampMillisSchema),
            @"{""logicalType"": ""timestamp-millis"", ""type"": ""long""}",
            @"timestamp-millis",
            TestName = "Timestamp - Millisecond"
        )]
        [TestCase(
            typeof(TimestampMicrosSchema),
            @"{""logicalType"": ""timestamp-micros"", ""type"": ""long""}",
            @"timestamp-micros",
            TestName = "Timestamp - Microsecond"
        )]
        [TestCase(
            typeof(TimestampNanosSchema),
            @"{""logicalType"": ""timestamp-nanos"", ""type"": ""long""}",
            @"timestamp-nanos",
            TestName = "Timestamp - Nanosecond"
        )]
        [TestCase(
            typeof(DurationSchema),
            @"{""logicalType"": ""duration"", ""type"": { ""type"": ""fixed"", ""name"": ""duration"", ""size"": 12}}",
            @"duration",
            TestName = "Duration"
        )]
        [TestCase(
            typeof(UuidSchema),
            @"{""logicalType"": ""uuid"", ""type"": ""string""}",
            @"uuid",
            TestName = "UUID - As String"
        )]
        [TestCase(
            typeof(StringSchema),
            @"{""logicalType"": ""customLogicalType"", ""type"": ""string""}",
            @"string",
            TestName = "CustomLogicalType - As String"
        )]
        public void SchemaParseBasic(Type expectedSchema, string avroString, string expectedToString)
        {
            var schema = AvroParser.ReadSchema(avroString);
            Assert.AreEqual(expectedSchema, schema.GetType());
            Assert.AreEqual(expectedToString, schema.ToString());
        }
    }
}
