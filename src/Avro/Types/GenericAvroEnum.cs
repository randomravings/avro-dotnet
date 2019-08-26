using Avro.Schemas;
using System;

namespace Avro.Types
{
    public class GenericAvroEnum : IEquatable<GenericAvroEnum>, IEquatable<string>
    {
        public GenericAvroEnum(EnumSchema schema, string symbol)
        {
            var value = schema.Symbols.IndexOf(symbol);
            if (value < 0)
                throw new ArgumentOutOfRangeException();
            Schema = schema;
            Value = value;
        }

        public GenericAvroEnum(EnumSchema schema, int value = 0)
        {
            if (value < 0 || value >= schema.Symbols.Count)
                throw new IndexOutOfRangeException();
            Schema = schema;
            Value = value;
        }

        public GenericAvroEnum(GenericAvroEnum e, bool copy = false)
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

        public bool Equals(GenericAvroEnum other)
        {
            return Symbol == other.Symbol;
        }

        public bool Equals(string other)
        {
            return Symbol == other;
        }

        public override string ToString() => Symbol;

        public static implicit operator int(GenericAvroEnum e) => e.Value;
    }
}
