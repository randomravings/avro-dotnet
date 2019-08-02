using Avro.Schemas;

namespace Avro.Generic
{
    public class GenericEnum 
    {
        public GenericEnum(EnumSchema schema, int value)
        {
            Schema = schema;
            Value = value;
        }
        public EnumSchema Schema { get; private set; }
        public int Value { get; private set; }
        public string Symbol => Schema.Symbols[Value];
    }
}
