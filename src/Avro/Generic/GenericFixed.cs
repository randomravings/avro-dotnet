using Avro.Schemas;
using System;

namespace Avro.Generic
{
    public class GenericFixed : IEquatable<GenericFixed>, IEquatable<byte[]>
    {
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

        public FixedSchema Schema { get; private set; }
        public int Size { get; private set; }
        public byte[] Value { get; private set; }

        public bool Equals(GenericFixed other)
        {
            return Equals(other.Value);
        }

        public bool Equals(byte[] other)
        {
            if (Value.Length != other.Length)
                return false;
            for (int i = 0; i < Value.Length; i++)
                if (Value[i] != other[i])
                    return false;
            return true;
        }
    }
}
