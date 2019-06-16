using System.Collections.Generic;

namespace Avro.Schemas
{
    public class TimeNanosSchema : LogicalSchema
    {
        public TimeNanosSchema()
            : this(new LongSchema()) { }

        public TimeNanosSchema(Schema type)
            : base(type, "time-nanos")
        {
            if (!(type is LongSchema))
                throw new SchemaParseException("Expected 'long' as type for logical type 'time-nanos'");
        }
    }
}
