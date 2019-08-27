namespace Avro.Schema
{
    public class FixedSchema : NamedSchema
    {
        private int _size = 0;

        public FixedSchema()
            : base() { }

        public FixedSchema(string name)
            : base(name) { }

        public FixedSchema(string name, string ns)
            : base(name, ns) { }

        public FixedSchema(string name, string ns, int size)
            : base(name, ns)
        {
            Size = size;
        }

        public int Size { get { return _size; } set { ValidateSize(value); _size = value; } }

        public override bool Equals(AvroSchema other)
        {
            return base.Equals(other) &&
                (other is FixedSchema) &&
                (other as FixedSchema).Size == Size
            ;
        }

        private static void ValidateSize(int size)
        {
            if (size < 0)
                throw new AvroParseException("Size for fixed type must zero or a positive number");
        }
    }
}
