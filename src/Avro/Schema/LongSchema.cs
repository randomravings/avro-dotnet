using Avro.Serialization;

namespace Avro.Schema
{
    [SerializationType(typeof(long))]
    public sealed class LongSchema : AvroSchema
    {
        public override string ToString() => "long";
    }
}
