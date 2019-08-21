using System;
using System.Collections.Generic;

namespace Avro.Specific
{
    public interface ISpecificFixed : IEquatable<ISpecificFixed>, IEnumerable<byte>
    {
        Schema Schema { get; }
        int Size { get; }
        byte this[int i] { get; set; }
    }
}
