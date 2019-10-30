using System;

namespace Avro.Schema
{
    public sealed class FixedSchema : NamedSchema, IEquatable<FixedSchema>
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

        public int Size { get { return _size; } set { _size = ValidateSize(value); } }

        public override bool Equals(object obj) => base.Equals(obj) && Equals((FixedSchema)obj);

        public bool Equals(FixedSchema other) => base.Equals(other) && Size == other.Size;

        public override int GetHashCode() => HashCode.Combine(base.GetHashCode(), Size);

        private static int ValidateSize(int size) =>
            size switch
            {
                var s when s < 0 => throw new AvroParseException("Size for fixed type must zero or a positive number"),
                _ => size
            };
    }
}
