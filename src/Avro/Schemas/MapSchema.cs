using System.Collections.Generic;

namespace Avro.Schemas
{
    public class MapSchema : Schema
    {
        public MapSchema(Schema values)
        {
            Values = values;
        }

        public Schema Values { get; set; }
        public override string ToString() => "map";
    }
}
