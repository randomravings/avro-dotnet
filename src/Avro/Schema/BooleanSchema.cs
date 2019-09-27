using Avro.Serialization;

namespace Avro.Schema
{
    [SerializationType(typeof(bool))]
    public sealed class BooleanSchema : AvroSchema
    {
        public override string ToString() => "boolean";
    }
}
