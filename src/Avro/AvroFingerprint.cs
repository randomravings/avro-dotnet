using System.Text;

namespace Avro
{
    public static class AvroFingerprint
    {
        private const long EMPTY = -4513414715797952619L;
        private static readonly long[] FP_TABLE = InitFPTable();

        public static long CRC64Value(AvroSchema s)
        {
            var bytes = Encoding.UTF8.GetBytes(s.ToAvroCanonical());
            return Fingerprint64(bytes);
        }

        private static long Fingerprint64(byte[] buf)
        {
            long fp = EMPTY;
            foreach (var b in buf)
                fp = ((long)(((ulong)fp) >> 8)) ^ FP_TABLE[(int)(fp ^ b) & 0xff];
            return fp;
        }

        private static long[] InitFPTable()
        {
            var fpt = new long[256];
            for (int i = 0; i < 256; i++)
            {
                long fp = i;
                for (int j = 0; j < 8; j++)
                {
                    long mask = -(fp & 1L);
                    fp = ((long)(((ulong)fp) >> 1)) ^ (EMPTY & mask);
                }
                fpt[i] = fp;
            }
            return fpt;
        }
    }
}
