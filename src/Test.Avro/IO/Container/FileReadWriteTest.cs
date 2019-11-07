using Avro;
using Avro.Container;
using Avro.IO;
using Avro.Schema;
using Avro.Types;
using NUnit.Framework;
using System.Diagnostics;
using System.IO;
using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;

namespace Test.Avro.IO.Container
{
    [TestFixture]
    public class FileReadWriteTest
    {
        private const string SCHEMA = @"
        {
            ""type"": ""record"", ""name"": ""test.Weather"",
            ""doc"": ""A weather reading."",
            ""fields"":
            [
                {""name"": ""station"", ""type"": ""string"", ""order"": ""ignore""},
                {""name"": ""time"", ""type"": ""long""},
                {""name"": ""temp"", ""type"": ""int""}
            ]
        }
        ";

        private const string DATA = @"
            {""station"":""011990-99999"",""time"":-619524000000,""temp"":0}
            {""station"":""011990-99999"",""time"":-619506000000,""temp"":22}
            {""station"":""011990-99999"",""time"":-619484400000,""temp"":-11}
            {""station"":""012650-99999"",""time"":-655531200000,""temp"":111}
            {""station"":""012650-99999"",""time"":-655509600000,""temp"":78}
        ";

        [TestCase]
        public void RunTest()
        {
            var schema = AvroParser.ReadSchema<RecordSchema>(SCHEMA);
            var reader = new DatumReader<GenericRecord>(schema);

            using var stream = new StringReader(DATA.Replace(" ", ""));
            using var decoder = new JsonDecoder(stream, schema);


            var records = new GenericRecord[5];
            for (int i = 0; i < records.Length; i++)
                records[i] = reader.Read(decoder);


            var fileSystem = new MockFileSystem();            
            fileSystem.AddDirectory("data");

            

            var fileUncompressed = new AvroFile(@"C:\data\test-file-null.avro", fileSystem);
            var fileCompressed = new AvroFile(@"C:\data\test-file-deflate.avro", fileSystem);

            using var writerUncompressed = fileUncompressed.OpenWrite<GenericRecord>(schema);
            using var writerCompressed = fileCompressed.OpenWrite<GenericRecord>(schema);

            foreach (var record in records)
            {
                writerUncompressed.Write(record);
                writerCompressed.Write(record);
            }

            writerUncompressed.Flush();
            writerCompressed.Flush();

            writerUncompressed.Close();
            writerCompressed.Close();

            using var readerUncompressed = fileUncompressed.OpenRead<GenericRecord>(schema);
            using var readerCompressed = fileCompressed.OpenRead<GenericRecord>(schema);

            Debug.WriteLine("");
            foreach (var block in readerUncompressed)
                foreach(var record in block)
                    Debug.WriteLine(record);

            Debug.WriteLine("");
            foreach (var block in readerCompressed)
                foreach (var record in block)
                    Debug.WriteLine(record);
        }
    }
}
