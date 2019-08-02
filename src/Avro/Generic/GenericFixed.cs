using Avro.Schemas;

namespace Avro.Generic
{
    public class GenericFixed
    {
        public GenericFixed(FixedSchema schema)
        {
            Schema = schema;
            Size = schema.Size;
            Value = new byte[schema.Size];
        }
        public FixedSchema Schema { get; private set; }
        public int Size { get; private set; }
        public byte[] Value { get; private set; }
    }
}
