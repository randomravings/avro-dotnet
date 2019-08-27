using Avro.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Avro.File
{
    public class FileHeader
    {
        private static readonly byte[] MAGIC_BYTES = new byte[] { 0x4F, 0x62, 0x6A, 0x01 };
        private FileInfo _fileInfo;

        private FileHeader(FileHeader header, FileInfo fileInfo)
        {
            _fileInfo = fileInfo;
            Magic = header.Magic.Clone() as byte[];
            Sync = header.Sync.Clone() as byte[];
            Codec = header.Codec;
            Schema = AvroParser.ReadSchema(header.Schema.ToAvro());
            FileHeaderSize = header.FileHeaderSize;
            Metadata = new Dictionary<string, byte[]>();

            foreach (var keyValue in header.Metadata)
            {
                var key = string.Copy(keyValue.Key);
                var value = keyValue.Value.Clone() as byte[];
                Metadata.Add(key, value);
            }
        }

        public FileHeader(FileInfo fileInfo)
        {
            FileInfo = fileInfo;
        }

        private void UpdateFromFile()
        {
            using (var stream = FileInfo.Open(FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var decoder = new BinaryDecoder(stream))
            {
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
        }

        private void Reset()
        {
            Magic = MAGIC_BYTES;
            Sync = Guid.NewGuid().ToByteArray();
            Metadata = new Dictionary<string, byte[]>();
        }

        public FileInfo FileInfo
        {
            get
            {
                return _fileInfo;
            }
            set
            {
                _fileInfo = value;
                if (_fileInfo.Exists)
                    UpdateFromFile();
                else
                    Reset();
            }
        }
        public byte[] Magic { get; set; }
        public byte[] Sync { get; set; }
        public Codec? Codec { get; set; }
        public AvroSchema Schema { get; set; }
        public IDictionary<string, byte[]> Metadata { get; set; }
        public long FileHeaderSize { get; set; }

        public FileHeader CloneNew(FileInfo fileInfo)
        {
            return new FileHeader(this, fileInfo);
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
