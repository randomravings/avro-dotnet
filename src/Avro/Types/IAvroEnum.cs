using Avro.Schema;
using System;

namespace Avro.Types
{
    public interface IAvroEnum : IEquatable<IAvroEnum>
    {
        EnumSchema Schema { get; }
        int Value { get; set; }
        string Symbol { get; set; }
    }
}
