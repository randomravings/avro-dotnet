using System.Collections.Generic;

namespace Avro.Schemas
{
    public class TimestampMicrosSchema : LogicalSchema
    {
        public TimestampMicrosSchema()
            : this(new LongSchema()) { }

        public TimestampMicrosSchema(Schema type)
            : base(type, "timestamp-micros")
        {
            if (!(type is LongSchema))
                throw new AvroParseException("Expected 'long' as type for logical type 'timestamp-micros'");
        }
    }
}
