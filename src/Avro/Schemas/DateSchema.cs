using System.Collections.Generic;

namespace Avro.Schemas
{
    public class DateSchema : LogicalSchema
    {
        public DateSchema()
            : base(new IntSchema(), "date") { }

        public DateSchema(Schema type)
            : base(type, "time-millis")
        {
            if (!(type is IntSchema))
                throw new AvroParseException("Expected 'int' as type for logical type 'date'");
        }
    }
}
