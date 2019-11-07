using Avro.IO;
using System;
using System.IO;

namespace Avro.Container
{
    public class Header
    {
        private Header() { }
        public Header(AvroSchema schema, Codec codec)
        {
            Metadata.Schema = schema;
            Metadata.Codec = codec;
        }

        public static Header FromStream(Stream stream)
        {
            var header = new Header();
            using var decoder = new BinaryDecoder(stream);
            header.Magic = decoder.ReadFixed(4);
            header.Metadata = decoder.ReadMap<Metadata, byte[]>(s => s.ReadBytes());
            header.Sync = decoder.ReadFixed(16);
            return header;
        }

        public static void WriteToStream(Stream stream, Header header)
        {
            using var encoder = new BinaryEncoder(stream);
            encoder.WriteFixed(header.Magic);
            encoder.WriteMap<Metadata, byte[]>(header.Metadata, (s, v) => s.WriteBytes(v));
            encoder.WriteFixed(header.Sync);
            stream.Flush();
        }

        public Magic Magic { get; set; } = new Magic();
        public Metadata Metadata { get; set; } = new Metadata();
        public AvroSchema Schema => Metadata.Schema;
        public Codec Codec => Metadata.Codec;
        public Sync Sync { get; set; } = new Sync();
    }
}
