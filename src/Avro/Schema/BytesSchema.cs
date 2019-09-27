using Avro.Serialization;

namespace Avro.Schema
{
    [SerializationType(typeof(byte[]), CompatibleTypes = new [] { typeof(string) })]
    public sealed class BytesSchema : AvroSchema
    {
        public override string ToString() => "bytes";
    }
}
