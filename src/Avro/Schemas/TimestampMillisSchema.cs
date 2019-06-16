using System.Collections.Generic;

namespace Avro.Schemas
{
    public class TimestampMillisSchema : LogicalSchema
    {
        public TimestampMillisSchema()
            : this(new LongSchema()) { }

        public TimestampMillisSchema(Schema type)
            : base(type, "timestamp-millis")
        {
            if (!(type is LongSchema))
                throw new SchemaParseException("Expected 'long' as type for logical type 'timestamp-micros'");
        }
    }
}
