using Avro.Serialization;
using System;

namespace Avro.Schema
{
    [SerializationType(typeof(TimeSpan))]
    public sealed class TimeMillisSchema : LogicalSchema
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
