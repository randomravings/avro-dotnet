using Avro;
using Avro.Schema;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace Test.Avro.Schema
{
    [TestFixture]
    public class SchemaParseExceptionTest
    {
        [TestCase(
            "1:abc",
            typeof(AvroParseException),
            TestName = "Invalid - JSON"
        )]
        [TestCase(
            "1",
            typeof(AvroParseException),
            TestName = "Number - Litteral"
        )]
        [TestCase(
            null,
            typeof(AvroParseException),
            TestName = "Null - Litteral"
        )]
        [TestCase(
            @"decimal",
            typeof(AvroParseException),
            TestName = "Unknown Type - Litteral"
        )]
        [TestCase(
            @"""decimal""",
            typeof(AvroParseException),
            TestName = "Unknown Type - String"
        )]
        [TestCase(
            @"{""type"": ""decimal""}",
            typeof(AvroParseException),
            TestName = "Unknown Type - Object"
        )]
        [TestCase(
            @"{""type"": ""enum"", ""symbols"" : [""I"", ""will"", ""fail"", ""no"", ""name""]}",
            typeof(KeyNotFoundException),
            TestName = "Enum - No name"
        )]
        [TestCase(
            @"{""type"": ""enum"", ""name"": [ 0, 1, 1, 2, 3, 5, 8 ], ""symbols"": [""Golden"", ""Mean""]}",
            typeof(InvalidCastException),
            TestName = "Enum - Name not a string"
        )]
        [TestCase(
            @"{""type"": ""enum"", ""name"": ""Status"", ""symbols"": ""Normal Caution Critical""}",
            typeof(InvalidCastException),
            TestName = "Enum - Symbols not an array"
        )]
        [TestCase(
            @"{""type"": ""enum"", ""name"": ""Test"", ""symbols"" : [""AA"", ""AA""]}",
            typeof(AvroParseException),
            TestName = "Enum - Duplicate symbol"
        )]
        [TestCase(
            @"[""string"", ""long"", ""long""]",
            typeof(AvroParseException),
            TestName = "Union - Duplicate Primitive Type"
        )]
        [TestCase(
            @"[{""type"": ""array"", ""items"": ""long""}, {""type"": ""array"", ""items"": ""string""}]",
            typeof(AvroParseException),
            TestName = "Union - Duplicate Array Type"
        )]
        [TestCase(
            @"[{""type"": ""array"", ""items"": ""long""}, {""type"": ""array"", ""items"": ""string""}]",
            typeof(AvroParseException),
            TestName = "Union - Duplicate Map Type"
        )]
        [TestCase(
            @"{""type"": ""fixed"", ""name"": ""Missing size""}",
            typeof(KeyNotFoundException),
            TestName = "Fixed - Missing Size"
        )]
        [TestCase(
            @"{""type"": ""fixed"", ""size"": 314}",
            typeof(KeyNotFoundException),
            TestName = "Fixed - No name"
        )]
        [TestCase(
            @"{""type"": ""record"",""name"":""LongList"",""fields"":[{""name"":""value"",""type"":""long""},{""name"":""next"",""type"":[""LongListA"",""null""]}]}",
            typeof(AvroParseException),
            TestName = "Record - Missing Name"
        )]
        [TestCase(
            @"{""type"": ""record"",""name"":""LongList""}",
            typeof(KeyNotFoundException),
            TestName = "Record - No fields"
        )]
        [TestCase(
            @"{""type"": ""record"",""name"":""LongList"", ""fields"": ""hi""}",
            typeof(InvalidCastException),
            TestName = "Record - Fields not an array"
        )]
        public void SchemaParseException(string avroString, Type expectedExceptionType)
        {
            Assert.Throws(expectedExceptionType, () => { AvroParser.ReadSchema(avroString); });
        }
    }
}
