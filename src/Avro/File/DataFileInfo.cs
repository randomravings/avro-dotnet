using Avro.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Avro.File
{
    public class DataFileInfo
    {
        private static readonly byte[] MAGIC_BYTES = new byte[] { 0x4F, 0x62, 0x6A, 0x01 };
        private FileInfo _fileInfo;

        private DataFileInfo(DataFileInfo dataFileInfo, FileInfo fileInfo)
        {
            _fileInfo = fileInfo;
            Magic = dataFileInfo.Magic.Clone() as byte[];
            Sync =  dataFileInfo.Sync.Clone() as byte[];
            Codec = string.Copy(dataFileInfo.Codec);
            Schema = AvroReader.ReadSchema(dataFileInfo.Schema.ToAvro());
            FileHeaderSize = dataFileInfo.FileHeaderSize;
            Metadata = new Dictionary<string, byte[]>();

            foreach(var keyValue in dataFileInfo.Metadata)
            {
                var key = string.Copy(keyValue.Key);
                var value = keyValue.Value.Clone() as byte[];
                Metadata.Add(key, value);
            }
        }

        public DataFileInfo(FileInfo fileInfo)
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
                    Schema = AvroReader.ReadSchema(Encoding.UTF8.GetString(schemaBytes));
                else
                    throw new Exception("Schema not found");

                if (Metadata.TryGetValue("avro.codec", out var codecBytes))
                    Codec = Encoding.UTF8.GetString(codecBytes);
                else
                    throw new Exception("Codec not found");

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
        public string Codec { get; set; }
        public Schema Schema { get; set; }
        public IDictionary<string, byte[]> Metadata { get; set; }
        public long FileHeaderSize { get; set; }

        public DataFileInfo CloneNew(FileInfo fileInfo)
        {
            return new DataFileInfo(this, fileInfo);
        }

        public IFileReader<T> OpenRead<T>(IDatumReader<T> datumReader)
        {
            return new DataFileReader<T>(this, datumReader);
        }

        public IFileWriter<T> OpenWrite<T>(IDatumWriter<T> datumWriter, long maxBlockCount = 1000)
        {
            return new DataFileWriter<T>(this, datumWriter, maxBlockCount);
        }
    }
}
