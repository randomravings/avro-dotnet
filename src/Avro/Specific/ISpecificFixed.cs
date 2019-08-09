using System;

namespace Avro.Specific
{
    public interface ISpecificFixed : IEquatable<byte[]>
    {
        Schema Schema { get; }
        int FixedSize { get; }
        byte[] Value { get; }
    }
}
