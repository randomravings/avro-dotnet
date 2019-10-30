using Avro.Schema;
using System;
using System.Collections.Generic;

namespace Avro.Types
{
    public interface IAvroFixed : IEquatable<IAvroFixed>, IEnumerable<byte>
    {
        FixedSchema Schema { get; }
        int Size { get; }
        byte this[int i] { get; set; }
        byte[] Value { get; }
    }
}
