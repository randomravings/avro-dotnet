using org.apache.avro.ipc;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Ipc.Utils
{
    public class HashCompare : IEqualityComparer<MD5>
    {
        public bool Equals(MD5 x, MD5 y)
        {
            return x.Equals(y);
        }

        public int GetHashCode(MD5 obj)
        {
            if (obj == null)
                return 0;
            return obj.Sum(r => r) % obj.Size;
        }
    }
}
