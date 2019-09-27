using Avro.Serialization;
using System;

namespace Avro.Schema
{
    [SerializationType(typeof(Guid))]
    public sealed class UuidSchema : LogicalSchema
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
