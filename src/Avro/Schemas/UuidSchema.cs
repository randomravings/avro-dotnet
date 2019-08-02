using System.Collections.Generic;

namespace Avro.Schemas
{
    public class UuidSchema : LogicalSchema
    {
        public UuidSchema()
            : this(new StringSchema()) { }

        public UuidSchema(Schema type)
            : base(type, "uuid")
        {
            if (!(type is StringSchema))
                throw new AvroParseException("Expected 'string' as type for logical type 'time-micros'");
        }
    }
}
