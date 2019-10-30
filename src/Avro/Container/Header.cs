using Avro.IO;
using Avro.Schema;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Avro.Container
{
    public class Header
    {
        private static readonly byte[] MAGIC_BYTES = new byte[] { 0x4F, 0x62, 0x6A, 0x01 };

        private Header(Header header, FileInfo fileInfo)
        {
            FileInfo = fileInfo;
            Magic = (byte[])header.Magic.Clone();
            Sync = (byte[])header.Sync.Clone();
            Codec = header.Codec;
            Schema = AvroParser.ReadSchema(header.Schema.ToAvro());
            FileHeaderSize = header.FileHeaderSize;
            Metadata = new Dictionary<string, byte[]>();

            foreach (var keyValue in header.Metadata)
            {
                var key = string.Copy(keyValue.Key);
                var value = (byte[])keyValue.Value.Clone();
                Metadata.Add(key, value);
            }
        }

        public Header(FileInfo fileInfo)
        {
            FileInfo = fileInfo;
            if (FileInfo.Exists)
                UpdateFromFile();
            else
                Reset();
        }

        private void UpdateFromFile()
        {
            using var stream = FileInfo.Open(FileMode.Open, FileAccess.Read, FileShare.Read);
            using var decoder = new BinaryDecoder(stream);
            Magic = decoder.ReadFixed(4);
            if (!Magic.SequenceEqual(MAGIC_BYTES))
                throw new Exception("Invalid Avro file");

            Metadata = decoder.ReadMap(s => s.ReadBytes());

            if (Metadata.TryGetValue("avro.schema", out var schemaBytes))
                Schema = AvroParser.ReadSchema(Encoding.UTF8.GetString(schemaBytes));
            else
                throw new Exception("Schema not found");

            if (Metadata.TryGetValue("avro.codec", out var codecBytes))
                if (Enum.TryParse<Codec>(Encoding.UTF8.GetString(codecBytes), true, out var codec))
                    Codec = codec;
                else
                    throw new Exception("Codec not found");
            else
                Codec = null;

            Sync = decoder.ReadFixed(16);

            FileHeaderSize = stream.Position;
        }

        private void Reset()
        {
            Magic = MAGIC_BYTES;
            Sync = Guid.NewGuid().ToByteArray();
            Metadata = new Dictionary<string, byte[]>();
        }

        public FileInfo FileInfo { get; private set; }
        public byte[] Magic { get; set; } = MAGIC_BYTES;
        public byte[] Sync { get; set; } = Guid.NewGuid().ToByteArray();
        public Codec? Codec { get; set; } = null;
        public AvroSchema Schema { get; set; } = new NullSchema();
        public IDictionary<string, byte[]> Metadata { get; set; } = new Dictionary<string, byte[]>();
        public long FileHeaderSize { get; set; } = 0;

        public Header CloneNew(FileInfo fileInfo)
        {
            return new Header(this, fileInfo);
        }

        public IAvroFileReader<T> OpenRead<T>(IAvroReader<T> datumReader)
        {
            return new FileReader<T>(this, datumReader);
        }

        public IAvroFileWriter<T> OpenWrite<T>(IAvroWriter<T> datumWriter, long maxBlockCount = 1000)
        {
            return new FileWriter<T>(this, datumWriter, maxBlockCount);
        }
    }
}
