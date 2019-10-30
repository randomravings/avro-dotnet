namespace Avro.Ipc.Utils
{
    public static class MessageFramingUtil
    {
        public static void EncodeLength(int length, byte[] array, int offset)
        {
            array[offset] = (byte)(length >> 24);
            array[offset + 1] = (byte)(length >> 16);
            array[offset + 2] = (byte)(length >> 8);
            array[offset + 3] = (byte)length;
        }

        public static int DecodeLength(byte[] array, int offset)
        {
            return
                (array[offset] << 24) |
                (array[offset + 1] << 16) |
                (array[offset + 2] << 8) |
                (array[offset + 3]);
        }
    }
}
