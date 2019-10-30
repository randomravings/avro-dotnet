using Avro;
using Avro.Schema;
using Avro.Types;
using System;
using System.Collections;
using System.Collections.Generic;

namespace org.apache.avro.ipc
{
    /// <summary></summary>
    [AvroNamedType("org.apache.avro.ipc", "MD5")]
    public class MD5 : IAvroFixed
    {
        public static readonly FixedSchema _SCHEMA = AvroParser.ReadSchema<FixedSchema>("{\"name\":\"org.apache.avro.ipc.MD5\",\"type\":\"fixed\",\"size\":16}");
        public const int _SIZE = 16;
        public MD5()
        {
            Value = new byte[_SIZE];
        }

        public MD5(byte[] value)
        {
            if (value.Length != _SIZE)
                throw new ArgumentException($"Array must be of size: {_SIZE}");
            Value = value;
        }

        public FixedSchema Schema => _SCHEMA;
        public int Size => _SIZE;
        public byte[] Value { get; private set; }
        public byte this[int i]
        {
            get => Value[i];
            set => Value[i] = value;
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
            foreach (var b in Value)
                yield return b;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public static implicit operator MD5(byte[] value) => new MD5(value);
        public static implicit operator byte[](MD5 value) => value.Value;
    }
}