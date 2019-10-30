namespace Avro.Schema
{
    public sealed class DurationSchema : LogicalSchema
    {
        public DurationSchema()
            : this(new FixedSchema("duration", string.Empty, 12)) { }

        public DurationSchema(AvroSchema type)
            : base(type, "duration")
        {
            if (!(type is FixedSchema) || ((FixedSchema)type).Size != 12)
                throw new AvroParseException("Expected 'fixed' with size '12' as type for logical type 'duration'");
        }
    }
}
