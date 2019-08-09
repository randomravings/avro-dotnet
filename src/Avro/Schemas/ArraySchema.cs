using System.Collections.Generic;

namespace Avro.Schemas
{
    public class ArraySchema : Schema
    {
        public ArraySchema(Schema items)
        {
            Items = items;
        }

        public Schema Items { get; set; }

        public override bool Equals(Schema other)
        {
            return base.Equals(other) &&
                (other as ArraySchema).Items.Equals(Items);
        }

        public override string ToString() => $"array";
    }
}
