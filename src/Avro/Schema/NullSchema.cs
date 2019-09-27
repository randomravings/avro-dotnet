using Avro.Serialization;

namespace Avro.Schema
{
    [SerializationType(typeof(NullSchema))]
    public sealed class NullSchema : AvroSchema
    {
        public override string ToString() => "null";
    }
}
