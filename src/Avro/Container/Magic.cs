using Avro.Schema;
using Avro.Types;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Container
{
    public class Magic : IAvroFixed
    {
        public static readonly byte[] MAGIC_BYTES = new byte[] { 0x4F, 0x62, 0x6A, 0x01 };
        private static readonly FixedSchema MAGICSCHEMA = AvroParser.ReadSchema<FixedSchema>(@"{ ""type"": ""fixed"", ""name"": ""Magic"", ""size"": 4 }");
        public Magic()
        {
            MAGIC_BYTES.CopyTo(Value, 0);
        }
        public Magic(byte[] bytes)
        {
            if (bytes.Length != Size)
                throw new ArgumentException($"Array must be of size: '{Size}'.");
            Value = bytes;
        }
        public bool IsValid => Value.SequenceEqual(MAGIC_BYTES);
        public byte this[int i] { get => Value[i]; set => Value[i] = value; }
        public FixedSchema Schema => MAGICSCHEMA;
        public int Size => 4;
        public byte[] Value { get; } = new byte[4];
        public bool Equals(IAvroFixed other) => other is Magic && this.SequenceEqual(other);
        public IEnumerator<byte> GetEnumerator() => Array.AsReadOnly(Value).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public static implicit operator Magic(byte[] bytes) => new Magic(bytes);
        public static implicit operator byte[](Magic sync) => sync.Value;
        public static implicit operator ReadOnlySpan<byte>(Magic sync) => sync.Value.AsSpan();
    }
}
