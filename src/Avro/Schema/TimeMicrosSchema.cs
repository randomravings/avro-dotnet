namespace Avro.Schema
{
    public sealed class TimeMicrosSchema : LogicalSchema
    {
        public TimeMicrosSchema()
            : this(new LongSchema()) { }

        public TimeMicrosSchema(AvroSchema type)
            : base(type, "time-micros")
        {
            if (!(type is LongSchema))
                throw new AvroParseException("Expected 'long' as type for logical type 'time-micros'");
        }
    }
}
