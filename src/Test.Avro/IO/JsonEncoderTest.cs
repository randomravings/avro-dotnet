using Avro;
using Avro.IO;
using Avro.Schema;
using Avro.Types;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Test.Avro.IO
{
    [TestFixture]
    public class JsonEncoderTest
    {
        [TestCase]
        public void TestSimple()
        {
            var schema = AvroParser.ReadSchema<RecordSchema>(@"{
                ""name"":""Test.Foobar.Record.Thing"",
                ""type"":""record"",
                ""fields"":[
                    {""name"":""ID"",""type"":""int""},
                    {""name"":""Name"",""type"":""string""},
                    {""name"":""Nuller"",""type"":""null""},
                    {""name"":""family"",""type"":{
                        ""type"":""map"",""values"":""string""
                    }},
                    {""name"":""stuff"",""type"":{
                        ""name"":""stuffs"",
                        ""type"":""record"",
                        ""fields"":[
                            {""name"":""key"",""type"":""bytes""},
                            {""name"":""keytype"",""type"":{
                                ""name"":""keyenum"",
                                ""type"":""enum"",
                                ""symbols"":[""ANALOG"",""DIGITAL""]
                            }}
                        ]
                    }},
                    {""name"":""NullableThing"",""type"":[""null"",""string""]},
                    {""name"":""Tags"",""type"":{
                        ""type"":""array"",""items"":""string""
                    }}
                ]
            }");

            var stuffSchema = (RecordSchema)schema.First(r => r.Name == "stuff").Type;
            var enumSchema = (EnumSchema)stuffSchema.First(r => r.Name == "keytype").Type;

            var genericRecord = new GenericRecord(schema);
            var writer = new DatumWriter<GenericRecord>(schema);
            var reader = new DatumReader<GenericRecord>(schema);

            var expected = new GenericRecord[10];

            var stringBuilder = new StringBuilder();
            using (var stream = new StringWriter(stringBuilder))
            using (var encoder = new JsonEncoder(stream, schema))
            {
                for (int i = 0; i < expected.Length; i++)
                {
                    var record = new GenericRecord(genericRecord);
                    record[0] = i;
                    record[1] = $"foo{i}";
                    record[2] = AvroNull.Value;
                    record[3] = new Dictionary<string, string>() { { "Brother", "John" } };

                    var stuffRecord = new GenericRecord(stuffSchema);
                    var keyEnum = new GenericEnum(enumSchema, i % enumSchema.Count);

                    stuffRecord[0] = Guid.NewGuid().ToByteArray();
                    stuffRecord[1] = keyEnum;

                    record[4] = stuffRecord;

                    record[5] = (i % 2) == 0 ? "ToNullOrNotToNull" : null;
                    record[6] = new List<string> { };
                    writer.Write(encoder, record);

                    expected[i] = record;
                }
            }
            var allJson = stringBuilder.ToString();

            using (var stream = new StringReader(allJson))
            using (var decoder = new JsonDecoder(stream, schema))
            {
                for (int i = 0; i < expected.Length; i++)
                {
                    var record = reader.Read(decoder);
                    Assert.AreEqual(expected[i], record);
                }
            }
        }
    }
}
