using Avro.Schema;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Avro.Types
{
    public class GenericFixed : IAvroFixed
    {
        private readonly byte[] _value;
        public GenericFixed(FixedSchema schema)
        {
            Schema = schema;
            Size = schema.Size;
            _value = new byte[schema.Size];
        }

        public GenericFixed(FixedSchema schema, byte[] value)
        {
            if (value.Length != schema.Size)
                throw new ArgumentException($"Array size mismatch, schema: {schema.Size}, value: {value.Length}");
            Schema = schema;
            Size = schema.Size;
            _value = value;
        }

        public GenericFixed(GenericFixed f)
        {
            Schema = f.Schema;
            Size = f.Size;
            _value = new byte[f.Size];
        }
        public byte this[int i] { get => _value[i]; set => _value[i] = value; }
        public FixedSchema Schema { get; private set; }
        public int Size { get; private set; }
        public bool Equals(IAvroFixed other)
        {
            if (Size != other.Size)
                return false;
            for (int i = 0; i < Size; i++)
                if (_value[i] != other[i])
                    return false;
            return true;
        }

        public IEnumerator<byte> GetEnumerator()
        {
            foreach (var b in _value)
                yield return b;
        }
        public static implicit operator byte[](GenericFixed f) => f._value;
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
