using System.Collections.Generic;

namespace Avro.Schemas
{
    public class TimestampNanosSchema : LogicalSchema
    {
        public TimestampNanosSchema()
            : this(new LongSchema()) { }

        public TimestampNanosSchema(Schema type)
            : base(type, "timestamp-nanos")
        {
            if (!(type is LongSchema))
                throw new AvroParseException("Expected 'long' as type for logical type 'timestamp-nanos'");
        }
    }
}
