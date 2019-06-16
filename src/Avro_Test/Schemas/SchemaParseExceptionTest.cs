using Avro;
using Avro.Schemas;
using NUnit.Framework;
using System;

namespace Schemas
{
    [TestFixture()]
    public class SchemaParseExceptionTest
    {
        [SetUp]
        public void Setup()
        {
        }

        [TestCase(
            @"decimal",
            typeof(SchemaParseException),
            TestName = "Unknown Type - Litteral"
        )]
        [TestCase(
            @"""decimal""",
            typeof(SchemaParseException),
            TestName = "Unknown Type - String"
        )]
        [TestCase(
            @"{""type"": ""decimal""}",
            typeof(SchemaParseException),
            TestName = "Unknown Type - Object"
        )]
        [TestCase(
            @"{""type"": ""enum"", ""symbols"" : [""I"", ""will"", ""fail"", ""no"", ""name""]}",
            typeof(SchemaParseException),
            TestName = "Enum - No name"
        )]
        [TestCase(
            @"{""type"": ""enum"", ""name"": [ 0, 1, 1, 2, 3, 5, 8 ], ""symbols"": [""Golden"", ""Mean""]}",
            typeof(SchemaParseException),
            TestName = "Enum - Name not a string"
        )]
        [TestCase(
            @"{""type"": ""enum"", ""name"": ""Status"", ""symbols"": ""Normal Caution Critical""}",
            typeof(SchemaParseException),
            TestName = "Enum - Symbols not an array"
        )]
        [TestCase(
            @"{""type"": ""enum"", ""name"": ""Test"", ""symbols"" : [""AA"", ""AA""]}",
            typeof(SchemaParseException),
            TestName = "Enum - Duplicate symbol"
        )]
        [TestCase(
            @"[""string"", ""long"", ""long""]",
            typeof(SchemaParseException),
            TestName = "Union - Duplicate Primitive Type"
        )]
        [TestCase(
            @"[{""type"": ""array"", ""items"": ""long""}, {""type"": ""array"", ""items"": ""string""}]",
            typeof(SchemaParseException),
            TestName = "Union - Duplicate Array Type"
        )]
        [TestCase(
            @"[{""type"": ""array"", ""items"": ""long""}, {""type"": ""array"", ""items"": ""string""}]",
            typeof(SchemaParseException),
            TestName = "Union - Duplicate Map Type"
        )]
        [TestCase(
            @"{""type"": ""fixed"", ""name"": ""Missing size""}",
            typeof(SchemaParseException),
            TestName = "Fixed - Missing Size"
        )]
        [TestCase(
            @"{""type"": ""fixed"", ""size"": 314}",
            typeof(SchemaParseException),
            TestName = "Fixed - No name"
        )]
        [TestCase(
            @"{""type"": ""record"",""name"":""LongList"",""fields"":[{""name"":""value"",""type"":""long""},{""name"":""next"",""type"":[""LongListA"",""null""]}]}",
            typeof(SchemaParseException),
            TestName = "Record - Missing Name"
        )]
        [TestCase(
            @"{""type"": ""record"",""name"":""LongList""}",
            typeof(SchemaParseException),
            TestName = "Record - No fields"
        )]
        [TestCase(
            @"{""type"": ""record"",""name"":""LongList"", ""fields"": ""hi""}",
            typeof(SchemaParseException),
            TestName = "Record - Fields not an array"
        )]
        public void SchemaParseException(string avroString, Type expectedExceptionType)
        {
            Assert.Throws(expectedExceptionType, () => { Schema.Parse(avroString); });
        }
    }
}
