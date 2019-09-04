using Avro.IO;
using Avro.Schema;
using Avro.Types;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace Avro.Test.IO
{
    [TestFixture]
    public class JsonEncoderTest
    {
        [TestCase]
        public void TestSimple()
        {
            var schema = AvroParser.ReadSchema(@"{
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
            }") as RecordSchema;

            var stuffSchema = schema.First(r => r.Name == "stuff").Type as RecordSchema;
            var enumSchema = stuffSchema.First(r => r.Name == "keytype").Type as EnumSchema;


            var genericRecord = new GenericRecord(schema);
            var reader = new DatumReader<GenericRecord>(schema);
            var writer = new DatumWriter<GenericRecord>(schema);


            var delimiter = Environment.NewLine;
            var stringBuilder = new StringBuilder();
            using (var stream = new StringWriter(stringBuilder))
            using (var encoder = new JsonEncoder(stream, schema, delimiter))
            {
                for (int i = 0; i < 10; i++)
                {
                    var record = new GenericRecord(genericRecord);
                    record[0] = i;
                    record[1] = $"foo{i}";
                    record[2] = null;
                    record[3] = new Dictionary<string, string>() { { "Brother", "John" } };

                    var stuffRecord = new GenericRecord(stuffSchema);
                    var keyEnum = new GenericEnum(enumSchema, i % enumSchema.Symbols.Count);

                    stuffRecord[0] = Guid.NewGuid().ToByteArray();
                    stuffRecord[1] = keyEnum;

                    record[4] = stuffRecord;

                    record[5] = (i % 2) == 0 ? "ToNullOrNotToNull" : null;
                    record[6] = new List<string> { };
                    writer.Write(encoder, record);

                    var singleJson = stringBuilder.ToString();
                }
            }
            var allJson = stringBuilder.ToString();


            using (var stream = new StringReader(allJson))
            using (var decoder = new JsonDecoder(stream, schema, delimiter))
            {
                for (int i = 0; i < 10; i++)
                {
                    var record = reader.Read(decoder);
                    Debug.WriteLine(record.ToString());
                }
            }
        }
    }
}
