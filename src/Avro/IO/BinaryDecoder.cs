using Avro.Types;
using Avro.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using System.Text;

namespace Avro.IO
{
    public sealed class BinaryDecoder : IAvroDecoder
    {
        private readonly Stream _stream;

        public BinaryDecoder(Stream stream)
        {
            _stream = stream;
        }

        public IList<T> ReadArray<T>(Func<IAvroDecoder, T> itemsReader)
        {
            var array = new List<T>();
            var len = 0L;
            do
            {
                len = ReadLong();
                if (len < 0)
                    len = ReadLong();
                for (int i = 0; i < len; i++)
                {
                    var value = itemsReader.Invoke(this);
                    array.Add(value);
                }
            }
            while (len != 0);
            return array;
        }

        public bool ReadArrayBlock<T>(Func<IAvroDecoder, T> itemsReader, out IList<T> array)
        {
            array = new List<T>();
            var len = ReadLong();
            if (len == 0)
                return false;
            if (len < 0)
                len = ReadLong();
            for (int i = 0; i < len; i++)
            {
                var value = itemsReader.Invoke(this);
                array.Add(value);
            }
            len = _stream.ReadByte();
            if (len == 0)
                return false;
            _stream.Seek(-1, SeekOrigin.Current);
            return true;
        }

        public bool ReadBoolean()
        {
            var b = (byte)_stream.ReadByte();
            if (b == 0)
                return false;
            return true;
        }

        public byte[] ReadBytes()
        {
            var len = ReadLong();
            var bytes = new byte[len];
            _stream.Read(bytes, 0, (int)len);
            return bytes;
        }

        public DateTime ReadDate()
        {
            var days = ReadInt();
            return Constants.UNIX_EPOCH.AddDays(days);
        }

        public byte[] ReadBytes(byte[] bytes)
        {
            var len = ReadLong();
            _stream.Read(bytes, 0, (int)len);
            return bytes;
        }

        public decimal ReadDecimal(int scale)
        {
            var bytes = ReadBytes();
            var unscaled = new BigInteger(bytes.AsSpan(), isBigEndian: true);
            var value = (decimal)unscaled / (decimal)Math.Pow(10, scale);
            return value;
        }

        public decimal ReadDecimal(int scale, int len)
        {
            var bytes = ReadFixed(len);
            var index = 0;
            var unscaled = new BigInteger(bytes.AsSpan(index), isBigEndian: true);
            var value = (decimal)unscaled / (long)Math.Pow(10, scale);
            return value;
        }

        public double ReadDouble()
        {
            var bytes = new byte[8];
            _stream.Read(bytes, 0, 8);
            var bits =
                (bytes[0] & 0xFFL) |
                ((bytes[1] & 0xFFL) << 8) |
                ((bytes[2] & 0xFFL) << 16) |
                ((bytes[3] & 0xFFL) << 24) |
                ((bytes[4] & 0xFFL) << 32) |
                ((bytes[5] & 0xFFL) << 40) |
                ((bytes[6] & 0xFFL) << 48) |
                ((bytes[7] & 0xFFL) << 56)
            ;
            return BitConverter.Int64BitsToDouble(bits);
        }

        public AvroDuration ReadDuration()
        {
            var mm =
                (uint)(_stream.ReadByte() & 0xFF) << 24 |
                (uint)(_stream.ReadByte() & 0xFF) << 16 |
                (uint)(_stream.ReadByte() & 0xFF) << 8 |
                (uint)(_stream.ReadByte() & 0xFF)
            ;

            var dd =
                (uint)(_stream.ReadByte() & 0xFF) << 24 |
                (uint)(_stream.ReadByte() & 0xFF) << 16 |
                (uint)(_stream.ReadByte() & 0xFF) << 8 |
                (uint)(_stream.ReadByte() & 0xFF)
            ;

            var ms =
                (uint)(_stream.ReadByte() & 0xFF) << 24 |
                (uint)(_stream.ReadByte() & 0xFF) << 16 |
                (uint)(_stream.ReadByte() & 0xFF) << 8 |
                (uint)(_stream.ReadByte() & 0xFF)
            ;

            return new AvroDuration(mm, dd, ms);
        }

        public byte[] ReadFixed(int len)
        {
            var bytes = new byte[len];
            _stream.Read(bytes, 0, bytes.Length);
            return bytes;
        }

        public byte[] ReadFixed(byte[] bytes)
        {
            _stream.Read(bytes, 0, bytes.Length);
            return bytes;
        }

        public float ReadFloat()
        {
            var bytes = new byte[4];
            _stream.Read(bytes, 0, 4);
            var bits =
                (bytes[0] & 0xFF) |
                ((bytes[1] & 0xFF) << 8) |
                ((bytes[2] & 0xFF) << 16) |
                ((bytes[3] & 0xFF) << 24)
            ;
            return BitConverter.Int32BitsToSingle(bits);
        }

        public int ReadInt()
        {
            var b = (byte)_stream.ReadByte();
            var n = b & 0x7FU;
            var shift = 7;
            while ((b & 0x80) != 0)
            {
                b = (byte)_stream.ReadByte();
                n |= (b & 0x7FU) << shift;
                shift += 7;
            }
            var value = (int)n;
            return (-(value & 0x01)) ^ ((value >> 1) & 0x7FFFFFFF);
        }

        public long ReadLong()
        {
            var b = (byte)_stream.ReadByte();
            var n = b & 0x7FUL;
            var shift = 7;
            while ((b & 0x80) != 0)
            {
                b = (byte)_stream.ReadByte();
                n |= (b & 0x7FUL) << shift;
                shift += 7;
            }
            var value = (long)n;
            return (-(value & 0x01L)) ^ ((value >> 1) & 0x7FFFFFFFFFFFFFFFL);
        }

        public IDictionary<string, T> ReadMap<T>(Func<IAvroDecoder, T> valuesReader)
        {
            var map = new Dictionary<string, T>() as IDictionary<string, T>;
            var len = 0L;
            do
            {
                len = ReadLong();
                if (len < 0)
                    len = ReadLong();
                for (int i = 0; i < len; i++)
                {
                    var key = ReadString();
                    var value = valuesReader.Invoke(this);
                    map.Add(key, value);
                }
            }
            while (len != 0);
            return map;
        }

        public bool ReadMapBlock<T>(Func<IAvroDecoder, T> valuesReader, out IDictionary<string, T> map)
        {
            map = new Dictionary<string, T>() as IDictionary<string, T>;
            var len = ReadLong();
            if (len == 0)
                return false;
            if (len < 0)
                len = ReadLong();
            for (int i = 0; i < len; i++)
            {
                var key = ReadString();
                var value = valuesReader.Invoke(this);
                map.Add(key, value);
            }
            len = _stream.ReadByte();
            if (len == 0)
                return false;
            _stream.Seek(-1, SeekOrigin.Current);
            return true;
        }

        public AvroNull ReadNull()
        {
            return new AvroNull();
        }

        public T ReadNullableObject<T>(Func<IAvroDecoder, T> reader, long nullIndex) where T : class
        {
            var index = ReadLong();
            if (index == nullIndex)
                return null;
            return reader.Invoke(this);
        }

        public T? ReadNullableValue<T>(Func<IAvroDecoder, T> reader, long nullIndex) where T : struct
        {
            var index = ReadLong();
            if (index == nullIndex)
                return null;
            return reader.Invoke(this);
        }

        public string ReadString()
        {
            var bytes = ReadBytes();
            return Encoding.UTF8.GetString(bytes);
        }

        public TimeSpan ReadTimeMS()
        {
            var val = ReadInt();
            return TimeSpan.FromMilliseconds(val);
        }

        public TimeSpan ReadTimeUS()
        {
            var val = ReadLong();
            return TimeSpan.FromTicks(val * TimeSpan.TicksPerMillisecond / 1000);
        }

        public TimeSpan ReadTimeNS()
        {
            var val = ReadLong();
            return TimeSpan.FromTicks(val);
        }

        public DateTime ReadTimestampMS()
        {
            var val = ReadLong();
            return Constants.UNIX_EPOCH.AddMilliseconds(val);
        }

        public DateTime ReadTimestampUS()
        {
            var val = ReadLong();
            return Constants.UNIX_EPOCH.AddTicks(val * (TimeSpan.TicksPerMillisecond / 1000));
        }

        public DateTime ReadTimestampNS()
        {
            var val = ReadLong();
            return Constants.UNIX_EPOCH.AddTicks(val);
        }

        public Guid ReadUuid()
        {
            var s = ReadString();
            return Guid.Parse(s);
        }

        public void SkipArray(Action<IAvroDecoder> itemsSkipper)
        {
            var len = 0L;
            do
            {
                len = ReadLong();
                if (len < 0)
                {
                    _stream.Seek(-1 * len, SeekOrigin.Current);
                    continue;
                }

                for (int i = 0; i < len; i++)
                    itemsSkipper.Invoke(this);
            }
            while (len != 0);
        }

        public void SkipBoolean()
        {
            _stream.Seek(1, SeekOrigin.Current);
        }

        public void SkipBytes()
        {
            var len = ReadLong();
            _stream.Seek(len, SeekOrigin.Current);
        }

        public void SkipDate()
        {
            SkipInt();
        }

        public void SkipDecimal()
        {
            SkipBytes();
        }

        public void SkipDecimal(int len)
        {
            SkipFixed(len);
        }

        public void SkipDouble()
        {
            _stream.Seek(8, SeekOrigin.Current);
        }

        public void SkipDuration()
        {
            _stream.Seek(12, SeekOrigin.Current);
        }

        public void SkipFixed(int len)
        {
            _stream.Seek(len, SeekOrigin.Current);
        }

        public void SkipFloat()
        {
            _stream.Seek(4, SeekOrigin.Current);
        }

        public void SkipInt()
        {
            var b = (byte)_stream.ReadByte();
            while ((b & 0x80) != 0)
                b = (byte)_stream.ReadByte();
        }

        public void SkipLong()
        {
            var b = (byte)_stream.ReadByte();
            while ((b & 0x80) != 0)
                b = (byte)_stream.ReadByte();
        }

        public void SkipMap(Action<IAvroDecoder> valuesSkipper)
        {
            var len = 0L;
            do
            {
                len = ReadLong();
                if (len < 0)
                {
                    _stream.Seek(-1 * len, SeekOrigin.Current);
                    continue;
                }

                for (int i = 0; i < len; i++)
                {
                    SkipString();
                    valuesSkipper.Invoke(this);
                }
            }
            while (len != 0);
        }

        public void SkipNull() { }

        public void SkipNullable(Action<IAvroDecoder> skipper, long nullIndex)
        {
            var index = ReadLong();
            if (index != nullIndex)
                skipper.Invoke(this);
        }

        public void SkipString()
        {
            var len = ReadLong();
            _stream.Seek(len, SeekOrigin.Current);
        }


        public void SkipTimeMS()
        {
            SkipInt();
        }

        public void SkipTimeUS()
        {
            SkipLong();
        }

        public void SkipTimeNS()
        {
            SkipLong();
        }

        public void SkipTimestampMS()
        {
            SkipLong();
        }

        public void SkipTimestampUS()
        {
            SkipLong();
        }

        public void SkipTimestampNS()
        {
            SkipLong();
        }

        public void SkipUuid()
        {
            SkipString();
        }

        public void Dispose() { }
    }
}
