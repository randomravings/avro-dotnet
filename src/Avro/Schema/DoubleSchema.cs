using Avro.Serialization;

namespace Avro.Schema
{
    [SerializationType(typeof(double))]
    public sealed class DoubleSchema : AvroSchema
    {
        public override string ToString() => "double";
    }
}
