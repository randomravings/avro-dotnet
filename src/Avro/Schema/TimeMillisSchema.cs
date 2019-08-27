using System.Collections.Generic;

namespace Avro.Schema
{
    public class TimeMillisSchema : LogicalSchema
    {
        public TimeMillisSchema()
            : this(new IntSchema()) { }

        public TimeMillisSchema(AvroSchema type)
            : base(type, "time-millis")
        {
            if (!(type is IntSchema))
                throw new AvroParseException("Expected 'int' as type for logical type 'time-millis'");
        }
    }
}
