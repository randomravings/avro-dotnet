using Avro.Schemas;
using Avro.Types;
using System;
using System.Collections;
using System.Collections.Generic;

namespace org.apache.avro.ipc
{
    /// <summary></summary>
    public class MD5 : IAvroFixed
    {
        public static readonly FixedSchema _SCHEMA = Avro.AvroSchema.Parse("{\"name\":\"org.apache.avro.ipc.MD5\",\"type\":\"fixed\",\"size\":16}") as FixedSchema;
        public const int _SIZE = 16;
        private readonly byte[] _value;
        public MD5()
        {
            _value = new byte[_SIZE];
        }

        public MD5(byte[] value)
        {
            if (value.Length != _SIZE)
                throw new ArgumentException($"Array must be of size: {_SIZE}");
            _value = value;
        }

        public FixedSchema Schema => _SCHEMA;
        public int Size => _SIZE;
        public byte this[int i]
        {
            get => _value[i];
            set => _value[i] = value;
        }

        public bool Equals(IAvroFixed other)
        {
            if (Size != other.Size)
                return false;
            for (int i = 0; i < Size; i++)
                if (this[i] != other[i])
                    return false;
            return true;
        }

        public IEnumerator<byte> GetEnumerator()
        {
            foreach (var b in _value)
                yield return b;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public static implicit operator MD5(byte[] value) => new MD5(value);
        public static explicit operator byte[](MD5 value) => value._value;
    }
}