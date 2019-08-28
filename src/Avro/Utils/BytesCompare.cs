using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Utils
{
    public class BytesCompare : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[] x, byte[] y)
        {
            if (x.Length != y.Length)
                return false;
            return x.SequenceEqual(y);
        }

        public int GetHashCode(byte[] obj)
        {
            if (obj == null)
                return 0;
            return obj.Sum(r => r) % obj.Length;
        }
    }
}
