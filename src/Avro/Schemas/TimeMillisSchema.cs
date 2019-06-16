using System.Collections.Generic;

namespace Avro.Schemas
{
    public class TimeMillisSchema : LogicalSchema
    {
        public TimeMillisSchema()
            : this(new IntSchema()) { }

        public TimeMillisSchema(Schema type)
            : base(type, "time-millis")
        {
            if (!(type is IntSchema))
                throw new SchemaParseException("Expected 'int' as type for logical type 'time-millis'");
        }
    }
}
