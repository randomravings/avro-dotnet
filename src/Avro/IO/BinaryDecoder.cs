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
        private readonly bool _leaveOpen;

        public BinaryDecoder(Stream stream, bool leaveOpen = true)
        {
            _stream = stream;
            _leaveOpen = leaveOpen;
        }

        public IList<T> ReadArray<T>(Func<IAvroDecoder, T> itemsReader) => ReadArray<List<T>, T>(itemsReader);
        public IList<T> ReadArrayBlock<T>(Func<IAvroDecoder, T> itemsReader) => ReadArrayBlock<List<T>, T>(itemsReader);
        public bool ReadArrayBlock<T>(Func<IAvroDecoder, T> itemsReader, ref IList<T> array) => ReadArrayBlock(itemsReader, ref array);
        public M ReadArray<M, T>(Func<IAvroDecoder, T> itemsReader) where M : notnull, IList<T>, new()
        {
            var array = new M();
            long len;
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

        public M ReadArrayBlock<M, T>(Func<IAvroDecoder, T> itemsReader) where M : notnull, IList<T>, new()
        {
            var array = new M();
            ReadArrayBlock(itemsReader, ref array);
            return array;
        }

        public bool ReadArrayBlock<M, T>(Func<IAvroDecoder, T> itemsReader, ref M array) where M : notnull, IList<T>
        {
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

        public T ReadEnum<T>() where T : struct, Enum => (T)(object)ReadInt();

        public T ReadEnum<T>(T value) where T : notnull, IAvroEnum
        {
            value.Value = ReadInt();
            return value;
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

        public T ReadFixed<T>(T bytes) where T : notnull, IAvroFixed
        {
            _stream.Read(bytes.Value, 0, bytes.Size);
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

        public IDictionary<string, T> ReadMap<T>(Func<IAvroDecoder, T> valuesReader) => ReadMap<Dictionary<string, T>, T>(valuesReader);
        public IDictionary<string, T> ReadMapBlock<T>(Func<IAvroDecoder, T> valuesReader) => ReadMapBlock<Dictionary<string, T>, T>(valuesReader);
        public bool ReadMapBlock<T>(Func<IAvroDecoder, T> valuesReader, ref IDictionary<string, T> map) => ReadMapBlock(valuesReader, ref map);

        public M ReadMap<M, T>(Func<IAvroDecoder, T> valuesReader) where M : notnull, IDictionary<string, T>, new()
        {
            var map = new M();
            long len;
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

        public M ReadMapBlock<M, T>(Func<IAvroDecoder, T> valuesReader) where M : notnull, IDictionary<string, T>, new()
        {
            var map = new M();
            ReadMapBlock(valuesReader, ref map);
            return map;
        }

        public bool ReadMapBlock<M, T>(Func<IAvroDecoder, T> valuesReader, ref M map) where M : notnull, IDictionary<string, T>
        {
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
            return AvroNull.Value;
        }

        public T? ReadNullableObject<T>(Func<IAvroDecoder, T> reader, long nullIndex) where T : class
        {
            var index = ReadLong();
            if (index == nullIndex)
                return default;
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

        public AvroUnion<T1> ReadUnion<T1>(
            Func<IAvroDecoder, T1> reader1
        )
            where T1 : notnull
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1>(reader1.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 1);
                    throw ex;
            }
        }

        public AvroUnion<T1, T2> ReadUnion<T1, T2>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2
        )
            where T1 : notnull
            where T2 : notnull
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1, T2>(reader1.Invoke(this));
                case 1:
                    return new AvroUnion<T1, T2>(reader2.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 1);
                    throw ex;
            }
        }

        public AvroUnion<T1, T2, T3> ReadUnion<T1, T2, T3>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2,
            Func<IAvroDecoder, T3> reader3
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1, T2, T3>(reader1.Invoke(this));
                case 1:
                    return new AvroUnion<T1, T2, T3>(reader2.Invoke(this));
                case 2:
                    return new AvroUnion<T1, T2, T3>(reader3.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 2);
                    throw ex;
            }
        }

        public AvroUnion<T1, T2, T3, T4> ReadUnion<T1, T2, T3, T4>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2,
            Func<IAvroDecoder, T3> reader3,
            Func<IAvroDecoder, T4> reader4
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
            where T4 : notnull
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1, T2, T3, T4>(reader1.Invoke(this));
                case 1:
                    return new AvroUnion<T1, T2, T3, T4>(reader2.Invoke(this));
                case 2:
                    return new AvroUnion<T1, T2, T3, T4>(reader3.Invoke(this));
                case 3:
                    return new AvroUnion<T1, T2, T3, T4>(reader4.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 3);
                    throw ex;
            }
        }

        public AvroUnion<T1, T2, T3, T4, T5> ReadUnion<T1, T2, T3, T4, T5>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2,
            Func<IAvroDecoder, T3> reader3,
            Func<IAvroDecoder, T4> reader4,
            Func<IAvroDecoder, T5> reader5
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
            where T4 : notnull
            where T5 : notnull
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1, T2, T3, T4, T5>(reader1.Invoke(this));
                case 1:
                    return new AvroUnion<T1, T2, T3, T4, T5>(reader2.Invoke(this));
                case 2:
                    return new AvroUnion<T1, T2, T3, T4, T5>(reader3.Invoke(this));
                case 3:
                    return new AvroUnion<T1, T2, T3, T4, T5>(reader4.Invoke(this));
                case 4:
                    return new AvroUnion<T1, T2, T3, T4, T5>(reader4.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 4);
                    throw ex;
            }
        }

        public void SkipArray(Action<IAvroDecoder> itemsSkipper)
        {
            long len;
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

        public void SkipBoolean() => _stream.Seek(1, SeekOrigin.Current);

        public void SkipBytes() => _stream.Seek(ReadLong(), SeekOrigin.Current);

        public void SkipDate() => SkipInt();

        public void SkipDecimal() => SkipBytes();

        public void SkipDecimal(int len) => SkipFixed(len);

        public void SkipDouble() => _stream.Seek(8, SeekOrigin.Current);

        public void SkipDuration() => _stream.Seek(12, SeekOrigin.Current);

        public void SkipEnum() => SkipInt();

        public void SkipFixed(int len) => _stream.Seek(len, SeekOrigin.Current);

        public void SkipFloat() => _stream.Seek(4, SeekOrigin.Current);

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
            long len;
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


        public void SkipTimeMS() => SkipInt();

        public void SkipTimeUS() => SkipLong();

        public void SkipTimeNS() => SkipLong();

        public void SkipTimestampMS() => SkipLong();

        public void SkipTimestampUS() => SkipLong();

        public void SkipTimestampNS() => SkipLong();

        public void SkipUuid() => SkipString();

        public void SkipUnion<T1>(
            Action<IAvroDecoder> skipper1
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    skipper1.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 1);
                    throw ex;
            }
        }

        public void SkipUnion<T1, T2>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    skipper1.Invoke(this);
                    break;
                case 1:
                    skipper2.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 1);
                    throw ex;
            }
        }

        public void SkipUnion<T1, T2, T3>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    skipper1.Invoke(this);
                    break;
                case 1:
                    skipper2.Invoke(this);
                    break;
                case 2:
                    skipper3.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 2);
                    throw ex;
            }
        }

        public void SkipUnion<T1, T2, T3, T4>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    skipper1.Invoke(this);
                    break;
                case 1:
                    skipper2.Invoke(this);
                    break;
                case 2:
                    skipper3.Invoke(this);
                    break;
                case 3:
                    skipper4.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 3);
                    throw ex;
            }
        }

        public void SkipUnion<T1, T2, T3, T4, T5>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4,
            Action<IAvroDecoder> skipper5
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    skipper1.Invoke(this);
                    break;
                case 1:
                    skipper2.Invoke(this);
                    break;
                case 2:
                    skipper3.Invoke(this);
                    break;
                case 3:
                    skipper4.Invoke(this);
                    break;
                case 4:
                    skipper5.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 4);
                    throw ex;
            }
        }

        private static IndexOutOfRangeException UnionIndexException(long index, long range)
        {
            return new IndexOutOfRangeException($"Union Index out of range: '{index}'. Valid range [{0}:{range}]");
        }

        public void Dispose()
        {
            if (!_leaveOpen && _stream != null)
                _stream.Dispose();
        }
    }
}
