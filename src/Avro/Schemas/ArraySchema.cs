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

        public override string ToString() => $"array";
    }
}
