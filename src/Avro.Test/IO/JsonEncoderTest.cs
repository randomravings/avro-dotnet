using Avro.IO;
using Avro.Schema;
using Avro.Types;
using NUnit.Framework;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
                    {""name"":""Tags"",""type"":{
                        ""type"":""array"",
                        ""items"":""string""
                    }},
                    {""name"":""Nuller"",""type"":""null""},
                    {""name"":""family"",""type"":{""type"":""map"",""values"":""string""}}
                ]
            }") as RecordSchema;

            var genericRecord = new GenericRecord(schema);
            var reader = new DatumReader<GenericRecord>(schema);
            var writer = new DatumWriter<GenericRecord>(schema);


            var delimiter = ",";
            var stringBuilder = new StringBuilder();
            using (var stream = new StringWriter(stringBuilder))
            using (var encoder = new JsonEncoder(stream, schema, delimiter))
            {
                for (int i = 0; i < 10; i++)
                {
                    var record = new GenericRecord(genericRecord);
                    record[0] = i;
                    record[1] = $"foo{i}";
                    record[2] = new List<string> { };
                    record[3] = null;
                    record[4] = new Dictionary<string, string>() { };
                    writer.Write(encoder, record);
                }
            }
            var json = stringBuilder.ToString();


            using (var stream = new StringReader(json))
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
