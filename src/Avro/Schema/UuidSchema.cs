using System.Collections.Generic;

namespace Avro.Schema
{
    public class UuidSchema : LogicalSchema
    {
        public UuidSchema()
            : this(new StringSchema()) { }

        public UuidSchema(AvroSchema type)
            : base(type, "uuid")
        {
            if (!(type is StringSchema))
                throw new AvroParseException("Expected 'string' as type for logical type 'uuid'");
        }
    }
}
