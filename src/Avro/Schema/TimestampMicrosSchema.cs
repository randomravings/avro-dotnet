using System.Collections.Generic;

namespace Avro.Schema
{
    public class TimestampMicrosSchema : LogicalSchema
    {
        public TimestampMicrosSchema()
            : this(new LongSchema()) { }

        public TimestampMicrosSchema(AvroSchema type)
            : base(type, "timestamp-micros")
        {
            if (!(type is LongSchema))
                throw new AvroParseException("Expected 'long' as type for logical type 'timestamp-micros'");
        }
    }
}
