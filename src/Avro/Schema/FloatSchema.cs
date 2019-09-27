using Avro.Serialization;

namespace Avro.Schema
{
    [SerializationType(typeof(float))]
    public sealed class FloatSchema : AvroSchema
    {
        public override string ToString() => "float";
    }
}
