using Avro.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using System.Text;

namespace Avro.IO
{
    public class BinaryEncoder : IEncoder
    {
        private Stream _stream;

        public BinaryEncoder(Stream stream)
        {
            _stream = stream;
        }

        public void WriteBoolean(bool value)
        {
            _stream.WriteByte((byte)(value ? 1 : 0));
        }

        public void WriteInt(int value)
        {
            var n = (uint)((value << 1) ^ (value >> 31));
            while ((n & ~0x7FU) != 0)
            {
                _stream.WriteByte((byte)((n & 0x7f) | 0x80));
                n >>= 7;
            }
            _stream.WriteByte((byte)n);
        }

        public void WriteLong(long value)
        {
            var n = (ulong)((value << 1) ^ (value >> 63));
            while ((n & ~0x7FUL) != 0)
            {
                _stream.WriteByte((byte)((n & 0x7f) | 0x80));
                n >>= 7;
            }
            _stream.WriteByte((byte)n);
        }

        public void WriteFloat(float value)
        {
            var bits = BitConverter.SingleToInt32Bits(value);
            var bytes = new byte[]
            {
                (byte)((bits) & 0xFF),
                (byte)((bits >> 8) & 0xFF),
                (byte)((bits >> 16) & 0xFF),
                (byte)((bits >> 24) & 0xFF)
            };
            _stream.Write(bytes, 0, 4);
        }

        public void WriteDouble(double value)
        {
            var bits = BitConverter.DoubleToInt64Bits(value);
            var bytes = new byte[]
            {
                (byte)((bits) & 0xFF),
                (byte)((bits >> 8) & 0xFF),
                (byte)((bits >> 16) & 0xFF),
                (byte)((bits >> 24) & 0xFF),
                (byte)((bits >> 32) & 0xFF),
                (byte)((bits >> 40) & 0xFF),
                (byte)((bits >> 48) & 0xFF),
                (byte)((bits >> 56) & 0xFF)
            };
            _stream.Write(bytes, 0, 8);
        }

        public void WriteBytes(byte[] value)
        {
            WriteLong(value.LongLength);
            _stream.Write(value, 0, value.Length);
        }

        public void WriteFixed(byte[] value)
        {
            _stream.Write(value, 0, value.Length);
        }

        public void WriteString(string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            WriteBytes(bytes);
        }

        public void WriteDecimal(decimal value)
        {
            var bits = decimal.GetBits(value);
            var scale = (bits[3] >> 16) & 0x7F;
            var bytes = new BigInteger(value * (int)Math.Pow(10, scale)).ToByteArray(isBigEndian: true);
            WriteBytes(bytes);
        }

        public void WriteDecimal(decimal value, int len)
        {
            var bits = decimal.GetBits(value);
            var scale = (bits[3] >> 16) & 0x7F;
            var bytes = new BigInteger(value * (int)Math.Pow(10, scale)).ToByteArray(isBigEndian: true);
            var fix = new byte[len];
            bytes.CopyTo(fix, 0);
            WriteFixed(fix);
        }

        public void WriteDate(DateTime value)
        {
            var days = (value - Constants.UNIX_EPOCH).Days;
            WriteInt(days);
        }

        public void WriteTimeMS(TimeSpan value)
        {
            var time = value.Subtract(TimeSpan.FromDays(value.Days));
            var milliseconds = (int)Math.Round(time.TotalMilliseconds);
            WriteInt(milliseconds);
        }

        public void WriteTimeUS(TimeSpan value)
        {
            var time = value.Subtract(TimeSpan.FromDays(value.Days));
            var microseconds = (long)Math.Round(time.TotalMilliseconds * 1000);
            WriteLong(microseconds);
        }

        public void WriteTimeNS(TimeSpan value)
        {
            var time = value.Subtract(TimeSpan.FromDays(value.Days));
            var nanosecond = time.Ticks;
            WriteLong(nanosecond);
        }

        public void WriteTimestampMS(DateTime value)
        {
            var time = (value - Constants.UNIX_EPOCH);
            var milliseconds = (long)Math.Round(time.TotalMilliseconds);
            WriteLong(milliseconds);
        }

        public void WriteTimestampUS(DateTime value)
        {
            var time = (value - Constants.UNIX_EPOCH);
            var microseconds = (long)Math.Round(time.TotalMilliseconds * 1000);
            WriteLong(microseconds);
        }

        public void WriteTimestampNS(DateTime value)
        {
            var time = (value - Constants.UNIX_EPOCH);
            var nanosecond = time.Ticks;
            WriteLong(nanosecond);
        }

        public void WriteDuration(Tuple<int, int, int> value)
        {
            WriteInt(value.Item1);
            WriteInt(value.Item2);
            WriteInt(value.Item3);
        }

        public void WriteUuid(Guid value)
        {
            var s = value.ToString();
            WriteString(s);
        }

        public void WriteArray<T>(IList<T> items, Action<IEncoder, T> itemsWriter)
        {
            WriteLong(items.Count);
            foreach (var item in items)
                itemsWriter.Invoke(this, item);
            WriteLong(0);
        }

        public void WriteMap<T>(IDictionary<string, T> keyValues, Action<IEncoder, T> valuesWriter)
        {
            WriteLong(keyValues.Count);
            foreach (var keyValue in keyValues)
            {
                WriteString(keyValue.Key);
                valuesWriter.Invoke(this, keyValue.Value);
            }
            WriteLong(0);
        }

        public void WriteNull() { }

        public void Dispose()
        {
            _stream = null;
        }
    }
}
