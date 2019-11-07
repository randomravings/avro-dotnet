using Avro.Schema;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Types
{
    public sealed class GenericFixed : IAvroFixed
    {
        public static GenericFixed Empty { get; } = new GenericFixed(AvroParser.ReadSchema<FixedSchema>(@"{""name"":""com.acme.void.fixed"",""size"":0}"));

        public GenericFixed(FixedSchema schema)
        {
            Schema = schema;
            Size = schema.Size;
            Value = new byte[schema.Size];
        }

        public GenericFixed(FixedSchema schema, byte[] value)
        {
            if (value.Length != schema.Size)
                throw new ArgumentException($"Array size mismatch, schema: {schema.Size}, value: {value.Length}");
            Schema = schema;
            Size = schema.Size;
            Value = value;
        }

        public GenericFixed(GenericFixed f)
        {
            Schema = f.Schema;
            Size = f.Size;
            Value = new byte[f.Size];
        }
        public FixedSchema Schema { get; private set; }
        public int Size { get; private set; }
        public byte this[int i] { get => Value[i]; set => Value[i] = value; }
        public byte[] Value { get; private set; }
        public override bool Equals(object obj) => obj != null && obj is IAvroFixed && Equals((IAvroFixed)obj);
        public bool Equals(IAvroFixed other)
        {
            if (Schema.Name != other.Schema.Name)
                return false;
            if (Size != other.Size)
                return false;
            for (int i = 0; i < Size; i++)
                if (this[i] != other[i])
                    return false;
            return true;
        }
        public IEnumerator<byte> GetEnumerator() => Value.Cast<byte>().GetEnumerator();
        public static implicit operator byte[](GenericFixed f) => f.Value;
        public static bool operator ==(GenericFixed left, GenericFixed right) => EqualityComparer<GenericFixed>.Default.Equals(left, right);
        public static bool operator !=(GenericFixed left, GenericFixed right) => !(left == right);
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public override int GetHashCode() => HashCode.Combine(Value);
    }
}
