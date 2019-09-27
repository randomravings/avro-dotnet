using Avro.Serialization;

namespace Avro.Schema
{
    [SerializationType(typeof(int))]
    public sealed class IntSchema : AvroSchema
    {
        public override string ToString() => "int";
    }
}
