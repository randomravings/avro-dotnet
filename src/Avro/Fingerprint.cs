using System.Security.Cryptography;
using System.Text;

namespace Avro
{
    public static class Fingerprint
    {
        private const long EMPTY = -4513414715797952619L;
        private static long[] FP_TABLE = null;

        public static long CRC64Value(Schema s)
        {
            var bytes = Encoding.UTF8.GetBytes(s.ToAvroCanonical());
            return Fingerprint64(bytes);
        }

        private static long Fingerprint64(byte[] buf)
        {
            if (FP_TABLE == null) InitFPTable();
            long fp = EMPTY;
            foreach (var b in buf)
                fp = ((long)(((ulong)fp) >> 8)) ^ FP_TABLE[(int)(fp ^ b) & 0xff];
            return fp;
        }

        private static void InitFPTable()
        {
            FP_TABLE = new long[256];
            for (int i = 0; i < 256; i++)
            {
                long fp = i;
                for (int j = 0; j < 8; j++)
                {
                    long mask = -(fp & 1L);
                    fp = ((long)(((ulong)fp) >> 1)) ^ (EMPTY & mask);
                }
                FP_TABLE[i] = fp;
            }
        }
    }
}
