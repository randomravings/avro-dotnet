using System;

namespace Avro.Schema
{
    public sealed class ArraySchema : AvroSchema, IEquatable<ArraySchema>
    {
        public ArraySchema(AvroSchema items)
        {
            Items = items;
        }

        public AvroSchema Items { get; set; }

        public override bool Equals(object other) => base.Equals(other) && Equals((ArraySchema)other);

        public override int GetHashCode() => HashCode.Combine(base.GetHashCode(), Items);

        public override string ToString() => $"array";

        public bool Equals(ArraySchema other) => other != null && Items.Equals(other.Items);
    }
}
