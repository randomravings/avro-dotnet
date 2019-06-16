using System.Collections.Generic;

namespace Avro.Schemas
{
    public class TimeMicrosSchema : LogicalSchema
    {
        public TimeMicrosSchema()
            : this(new LongSchema()) { }

        public TimeMicrosSchema(Schema type)
            : base(type, "time-micros")
        {
            if (!(type is LongSchema))
                throw new SchemaParseException("Expected 'long' as type for logical type 'time-micros'");
        }
    }
}
