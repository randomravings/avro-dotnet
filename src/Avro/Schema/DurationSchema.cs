namespace Avro.Schema
{
    public class DurationSchema : LogicalSchema
    {
        public DurationSchema()
            : this(new FixedSchema("duration", null, 12)) { }

        public DurationSchema(AvroSchema type)
            : base(type, "duration")
        {
            if (!(type is FixedSchema) || (type as FixedSchema).Size != 12)
                throw new AvroParseException("Expected 'fixed' with size '12' as type for logical type 'duration'");
        }
    }
}
