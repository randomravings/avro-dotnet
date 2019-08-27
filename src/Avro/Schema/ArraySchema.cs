namespace Avro.Schema
{
    public class ArraySchema : AvroSchema
    {
        public ArraySchema(AvroSchema items)
        {
            Items = items;
        }

        public AvroSchema Items { get; set; }

        public override bool Equals(AvroSchema other)
        {
            return base.Equals(other) &&
                (other as ArraySchema).Items.Equals(Items);
        }

        public override string ToString() => $"array";
    }
}
