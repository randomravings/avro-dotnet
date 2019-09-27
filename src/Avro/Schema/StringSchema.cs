using Avro.Serialization;

namespace Avro.Schema
{
    [SerializationType(typeof(string), CompatibleTypes = new [] { typeof(byte[]) })]
    public class StringSchema : AvroSchema
    {
        public sealed override string ToString() => "string";
    }
}
