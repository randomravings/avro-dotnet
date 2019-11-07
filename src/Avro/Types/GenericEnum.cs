using Avro.Schema;
using System;
using System.Collections.Generic;

namespace Avro.Types
{
    public sealed class GenericEnum : IAvroEnum, IEquatable<IAvroEnum>
    {
        public static GenericEnum Empty { get; } = new GenericEnum(AvroParser.ReadSchema<EnumSchema>(@"{""name"":""com.acme.void.enum"",""symbols"":[]}"));
        private int _value;
        public GenericEnum(EnumSchema schema)
        {
            Schema = schema;
            Value = 0;
        }
        public GenericEnum(EnumSchema schema, int value)
        {
            Schema = schema;
            Value = value;
        }
        public GenericEnum(EnumSchema schema, string symbol)
        {
            Schema = schema;
            Symbol = symbol;
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
        public int Value
        {
            get
            {
                return _value;
            }
            set
            {
                if (value < 0 || value >= Schema.Count)
                    throw new IndexOutOfRangeException();
                _value = value;
            }
        }
        public string Symbol { get => Schema [_value]; set => _value = Schema[value]; }
        public override bool Equals(object obj) => obj != null && obj is GenericEnum && Equals((GenericEnum)obj);
        public bool Equals(IAvroEnum other) => Schema.Name == other.Schema.Name && Symbol == other.Symbol;
        public override int GetHashCode() => HashCode.Combine(Value, Symbol);
        public override string ToString() => Symbol;
        public static bool operator ==(GenericEnum left, GenericEnum right) => EqualityComparer<GenericEnum>.Default.Equals(left, right);
        public static bool operator !=(GenericEnum left, GenericEnum right) => !(left == right);
        public static implicit operator int(GenericEnum e) => e.Value;
        public static implicit operator string(GenericEnum e) => e.Symbol;
    }
}
