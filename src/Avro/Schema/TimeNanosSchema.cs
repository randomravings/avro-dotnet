namespace Avro.Schema
{
    public sealed class TimeNanosSchema : LogicalSchema
    {
        public TimeNanosSchema()
            : this(new LongSchema()) { }

        public TimeNanosSchema(AvroSchema type)
            : base(type, "time-nanos")
        {
            if (!(type is LongSchema))
                throw new AvroParseException("Expected 'long' as type for logical type 'time-nanos'");
        }
    }
}
