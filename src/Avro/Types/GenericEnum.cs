using Avro.Schema;
using System;

namespace Avro.Types
{
    public class GenericEnum : IEquatable<GenericEnum>, IEquatable<string>
    {
        public GenericEnum(EnumSchema schema, string symbol)
        {
            var value = schema.Symbols.IndexOf(symbol);
            if (value < 0)
                throw new ArgumentOutOfRangeException();
            Schema = schema;
            Value = value;
        }

        public GenericEnum(EnumSchema schema, int value = 0)
        {
            if (value < 0 || value >= schema.Symbols.Count)
                throw new IndexOutOfRangeException();
            Schema = schema;
            Value = value;
        }

        public GenericEnum(GenericEnum e, bool copy = false)
        {
            Schema = e.Schema;
            if (copy)
                Value = e.Value;
            else
                Value = 0;
        }

        public EnumSchema Schema { get; private set; }
        public int Value { get; private set; }
        public string Symbol => Schema.Symbols[Value];

        public bool Equals(GenericEnum other)
        {
            return Symbol == other.Symbol;
        }

        public bool Equals(string other)
        {
            return Symbol == other;
        }

        public override string ToString() => Symbol;

        public static implicit operator int(GenericEnum e) => e.Value;
    }
}
