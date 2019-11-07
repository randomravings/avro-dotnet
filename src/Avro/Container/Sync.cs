using Avro.Schema;
using Avro.Types;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Container
{
    public class Sync : IAvroFixed
    {
        private static readonly FixedSchema SYNCSCHEMA = AvroParser.ReadSchema<FixedSchema>(@"{ ""type"": ""fixed"", ""name"": ""Sync"", ""size"": 16 }");
        public Sync()
        {
            Value = Guid.NewGuid().ToByteArray();
        }
        public Sync(byte[] bytes)
        {
            if (bytes.Length != Size)
                throw new ArgumentException($"Array must be of size: '{Size}'.");
            Value = bytes;
        }
        public byte this[int i] { get => Value[i]; set => Value[i] = value; }
        public FixedSchema Schema => SYNCSCHEMA;
        public int Size => 16;
        public byte[] Value { get; } = new byte[16];
        public bool Equals(IAvroFixed other) => other is Magic && this.SequenceEqual(other);
        public IEnumerator<byte> GetEnumerator() => Array.AsReadOnly(Value).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public static implicit operator Sync(byte[] bytes) => new Sync(bytes);
        public static implicit operator byte[](Sync sync) => sync.Value;
        public static implicit operator ReadOnlySpan<byte>(Sync sync) => sync.Value.AsSpan();
    }
}
