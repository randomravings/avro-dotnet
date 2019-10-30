using Avro;
using Avro.IO;
using Avro.Schema;
using Avro.Types;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using System.IO;

namespace Test.Avro.Perf
{
    public sealed class Program
    {
        private const string SCHEMA = @"
            {
                ""type"": ""record"",
                ""namespace"": ""Test.Avro.Perf"",
                ""name"": ""TestRecord"",
                ""fields"": [
                    { ""name"": ""Bool"", ""type"": ""boolean"" },
                    { ""name"": ""Int32"", ""type"": ""int"" },
                    { ""name"": ""Int64"", ""type"": ""long"" },
                    { ""name"": ""Single"", ""type"": ""float"" },
                    { ""name"": ""Double"", ""type"": ""double"" },
                    { ""name"": ""String"", ""type"": ""string"" }
                ]
            }
        ";

        [RPlotExporter, RankColumn]
        public class GenericSerializer
        {
            private readonly RecordSchema _schema;
            private readonly IAvroRecord _genericRecord;
            private readonly TestRecord _specificRecord;
            private readonly IAvroWriter<IAvroRecord> _genericWriter;
            private readonly IAvroWriter<TestRecord> _specificWriter;

            public GenericSerializer()
            {
                _schema = AvroParser.ReadSchema<RecordSchema>(SCHEMA);
                _specificRecord = new TestRecord();
                _genericRecord = new GenericRecord(_schema);
                _genericWriter = new DatumWriter<IAvroRecord>(_schema);
                _specificWriter = new DatumWriter<TestRecord>(_schema);
            }

            [Params(1000, 10000)]
            public int N;

            [GlobalSetup]
            public void Setup()
            {
                _genericRecord[0] = true;
                _genericRecord[1] = 123;
                _genericRecord[2] = 4000000000L;
                _genericRecord[3] = float.MinValue;
                _genericRecord[4] = double.MaxValue;
                _genericRecord[5] = "Hello World!";


                _specificRecord.Bool = true;
                _specificRecord.Int32 = 123;
                _specificRecord.Int64 = 4000000000L;
                _specificRecord.Single = float.MinValue;
                _specificRecord.Double = double.MaxValue;
                _specificRecord.String = "Hello World!";
            }

            [Benchmark]
            public void WriteGeneric()
            {
                using var stream = new MemoryStream();
                using var encoder = new BinaryEncoder(stream);
                for (int i = 0; i < N; i++)
                    _genericWriter.Write(encoder, _genericRecord);
            }

            [Benchmark]
            public void WriteSpecific()
            {
                using var stream = new MemoryStream();
                using var encoder = new BinaryEncoder(stream);
                for (int i = 0; i < N; i++)
                    _specificWriter.Write(encoder, _specificRecord);
            }
        }

        static void Main()
        {
            BenchmarkRunner.Run<GenericSerializer>();
        }
    }
}
