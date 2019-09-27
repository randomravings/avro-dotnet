using Avro.IO.Formatting;
using Avro.Types;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Avro.IO
{
    public class JsonDecoder : IAvroDecoder
    {
        private readonly TextReader _stream;
        private readonly JsonTextReader _reader;
        private readonly string _delimiter;
        private readonly AvroJsonFormatter.Reader[] _actions;
        private int _index = 0;

        public JsonDecoder(TextReader stream, AvroSchema schema, string delimiter = null)
        {
            _stream = stream;
            _reader = new JsonTextReader(_stream) { SupportMultipleContent = true };
            _delimiter = delimiter;
            _actions = AvroJsonFormatter.GetReadActions(schema);

            _reader.Read();
        }

        public IList<T> ReadArray<T>(Func<IAvroDecoder, T> itemsReader)
        {
            var end = Advance(false, out _);
            var loop = _index;
            var items = new List<T>();
            while (_reader.TokenType != JsonToken.EndArray)
            {
                _index = loop;
                items.Add(itemsReader.Invoke(this));
            }
            _index = end;
            Advance(false, out _);
            return items;
        }

        public bool ReadArrayBlock<T>(Func<IAvroDecoder, T> itemsReader, out IList<T> array)
        {
            array = ReadArray(itemsReader);
            return false;
        }

        public bool ReadBoolean()
        {
            Advance(false, out var value);
            return bool.Parse(value);
        }

        public byte[] ReadBytes()
        {
            Advance(true, out var value);
            return value.Split(new string[] { "\\u00" }, StringSplitOptions.RemoveEmptyEntries).Select(r => Convert.ToByte(r, 16)).ToArray();
        }

        public DateTime ReadDate()
        {
            Advance(true, out var value);
            return DateTime.Parse(value);
        }

        public decimal ReadDecimal(int scale)
        {
            Advance(false, out var value);
            return decimal.Parse(value);
        }

        public decimal ReadDecimal(int scale, int len)
        {
            Advance(false, out var value);
            return decimal.Parse(value);
        }

        public double ReadDouble()
        {
            Advance(false, out var value);
            return double.Parse(value);
        }

        public AvroDuration ReadDuration()
        {
            Advance(true, out var value);
            var bytes = value.Split(new string[] { "\\u00" }, StringSplitOptions.RemoveEmptyEntries).Select(r => Convert.ToByte(r)).ToArray();

            var mm =
                (uint)(bytes[0] & 0xFF) << 24 |
                (uint)(bytes[1] & 0xFF) << 16 |
                (uint)(bytes[2] & 0xFF) << 8 |
                (uint)(bytes[3] & 0xFF)
            ;

            var dd =
                (uint)(bytes[4] & 0xFF) << 24 |
                (uint)(bytes[5] & 0xFF) << 16 |
                (uint)(bytes[6] & 0xFF) << 8 |
                (uint)(bytes[7] & 0xFF)
            ;

            var ms =
                (uint)(bytes[8] & 0xFF) << 24 |
                (uint)(bytes[9] & 0xFF) << 16 |
                (uint)(bytes[10] & 0xFF) << 8 |
                (uint)(bytes[11] & 0xFF)
            ;

            return new AvroDuration(mm, dd, ms);
        }

        public byte[] ReadFixed(byte[] bytes)
        {
            Advance(true, out var value);
            return value.Split(new string[] { "\\u00" }, StringSplitOptions.RemoveEmptyEntries).Select(r => Convert.ToByte(r)).ToArray();
        }

        public byte[] ReadFixed(int len)
        {
            Advance(true, out var value);
            return value.Split(new string[] { "\\u00" }, StringSplitOptions.RemoveEmptyEntries).Select(r => Convert.ToByte(r)).ToArray();
        }

        public float ReadFloat()
        {
            Advance(false, out var value);
            return float.Parse(value);
        }

        public int ReadInt()
        {
            Advance(false, out var value);
            return int.Parse(value);
        }

        public long ReadLong()
        {
            Advance(false, out var value);
            return long.Parse(value);
        }

        public IDictionary<string, T> ReadMap<T>(Func<IAvroDecoder, T> valuesReader)
        {
            var end = Advance(false, out _);
            var loop = _index;
            var items = new Dictionary<string, T>();
            while (true)
            {
                _index = loop;
                if (Advance(true, out var key) == 0)
                    break;
                var value = valuesReader.Invoke(this);
                items.Add(key, value);
            }
            _index = end;
            Advance(false, out _);
            return items;
        }

        public bool ReadMapBlock<T>(Func<IAvroDecoder, T> valuesReader, out IDictionary<string, T> map)
        {
            map = ReadMap(valuesReader);
            return false;
        }

        public AvroNull ReadNull()
        {
            Advance(false, out _);
            return AvroNull.Value;
        }

        public T ReadNullableObject<T>(Func<IAvroDecoder, T> reader, long nullIndex) where T : class
        {
            Advance(false, out var index);
            if (index == nullIndex.ToString())
            {
                _index++;
                return null;
            }
            else
            {
                return reader.Invoke(this);
            }
        }

        public T? ReadNullableValue<T>(Func<IAvroDecoder, T> reader, long nullIndex) where T : struct
        {
            Advance(false, out var index);
            if (index == nullIndex.ToString())
            {
                _index++;
                return null;
            }
            else
            {
                return reader.Invoke(this);
            }
        }

        public string ReadString()
        {
            Advance(true, out var value);
            return value;
        }

        public TimeSpan ReadTimeMS()
        {
            Advance(true, out var value);
            return TimeSpan.Parse(value);
        }

        public TimeSpan ReadTimeNS()
        {
            Advance(true, out var value);
            return TimeSpan.Parse(value);
        }

        public TimeSpan ReadTimeUS()
        {
            Advance(true, out var value);
            return TimeSpan.Parse(value);
        }

        public DateTime ReadTimestampMS()
        {
            Advance(true, out var value);
            return DateTime.Parse(value);
        }

        public DateTime ReadTimestampNS()
        {
            Advance(true, out var value);
            return DateTime.Parse(value);
        }

        public DateTime ReadTimestampUS()
        {
            Advance(true, out var value);
            return DateTime.Parse(value);
        }

        public Guid ReadUuid()
        {
            Advance(true, out var value);
            return Guid.Parse(value);
        }

        public AvroUnion<T1, T2> ReadUnion<T1, T2>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2
        )
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

        public AvroUnion<T1, T2, T3, T4, T5, T6> ReadUnion<T1, T2, T3, T4, T5, T6>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2,
            Func<IAvroDecoder, T3> reader3,
            Func<IAvroDecoder, T4> reader4,
            Func<IAvroDecoder, T5> reader5,
            Func<IAvroDecoder, T6> reader6
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6>(reader1.Invoke(this));
                case 1:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6>(reader2.Invoke(this));
                case 2:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6>(reader3.Invoke(this));
                case 3:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6>(reader4.Invoke(this));
                case 4:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6>(reader4.Invoke(this));
                case 5:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6>(reader5.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 5);
                    throw ex;
            }
        }

        public AvroUnion<T1, T2, T3, T4, T5, T6, T7> ReadUnion<T1, T2, T3, T4, T5, T6, T7>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2,
            Func<IAvroDecoder, T3> reader3,
            Func<IAvroDecoder, T4> reader4,
            Func<IAvroDecoder, T5> reader5,
            Func<IAvroDecoder, T6> reader6,
            Func<IAvroDecoder, T7> reader7
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(reader1.Invoke(this));
                case 1:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(reader2.Invoke(this));
                case 2:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(reader3.Invoke(this));
                case 3:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(reader4.Invoke(this));
                case 4:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(reader4.Invoke(this));
                case 5:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(reader5.Invoke(this));
                case 6:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(reader6.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 6);
                    throw ex;
            }
        }

        public AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> ReadUnion<T1, T2, T3, T4, T5, T6, T7, T8>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2,
            Func<IAvroDecoder, T3> reader3,
            Func<IAvroDecoder, T4> reader4,
            Func<IAvroDecoder, T5> reader5,
            Func<IAvroDecoder, T6> reader6,
            Func<IAvroDecoder, T7> reader7,
            Func<IAvroDecoder, T8> reader8
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(reader1.Invoke(this));
                case 1:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(reader2.Invoke(this));
                case 2:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(reader3.Invoke(this));
                case 3:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(reader4.Invoke(this));
                case 4:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(reader4.Invoke(this));
                case 5:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(reader5.Invoke(this));
                case 6:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(reader6.Invoke(this));
                case 7:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(reader7.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 7);
                    throw ex;
            }
        }

        public AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> ReadUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2,
            Func<IAvroDecoder, T3> reader3,
            Func<IAvroDecoder, T4> reader4,
            Func<IAvroDecoder, T5> reader5,
            Func<IAvroDecoder, T6> reader6,
            Func<IAvroDecoder, T7> reader7,
            Func<IAvroDecoder, T8> reader8,
            Func<IAvroDecoder, T9> reader9
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(reader1.Invoke(this));
                case 1:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(reader2.Invoke(this));
                case 2:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(reader3.Invoke(this));
                case 3:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(reader4.Invoke(this));
                case 4:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(reader4.Invoke(this));
                case 5:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(reader5.Invoke(this));
                case 6:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(reader6.Invoke(this));
                case 7:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(reader7.Invoke(this));
                case 8:
                    return new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(reader8.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 8);
                    throw ex;
            }
        }

        public void SkipArray(Action<IAvroDecoder> itemsSkipper)
        {
            throw new NotImplementedException();
        }

        public void SkipBoolean()
        {
            throw new NotImplementedException();
        }

        public void SkipBytes()
        {
            throw new NotImplementedException();
        }

        public void SkipDate()
        {
            throw new NotImplementedException();
        }

        public void SkipDecimal()
        {
            throw new NotImplementedException();
        }

        public void SkipDecimal(int len)
        {
            throw new NotImplementedException();
        }

        public void SkipDouble()
        {
            throw new NotImplementedException();
        }

        public void SkipDuration()
        {
            throw new NotImplementedException();
        }

        public void SkipFixed(int len)
        {
            throw new NotImplementedException();
        }

        public void SkipFloat()
        {
            throw new NotImplementedException();
        }

        public void SkipInt()
        {
            throw new NotImplementedException();
        }

        public void SkipLong()
        {
            throw new NotImplementedException();
        }

        public void SkipMap(Action<IAvroDecoder> valuesSkipper)
        {
            throw new NotImplementedException();
        }

        public void SkipNull()
        {
            throw new NotImplementedException();
        }

        public void SkipNullable(Action<IAvroDecoder> skipper, long nullIndex)
        {
            throw new NotImplementedException();
        }

        public void SkipString()
        {
            throw new NotImplementedException();
        }

        public void SkipTimeMS()
        {
            throw new NotImplementedException();
        }

        public void SkipTimeNS()
        {
            throw new NotImplementedException();
        }

        public void SkipTimestampMS()
        {
            throw new NotImplementedException();
        }

        public void SkipTimestampNS()
        {
            throw new NotImplementedException();
        }

        public void SkipTimestampUS()
        {
            throw new NotImplementedException();
        }

        public void SkipTimeUS()
        {
            throw new NotImplementedException();
        }

        public void SkipUuid()
        {
            throw new NotImplementedException();
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

        public void SkipUnion<T1, T2, T3, T4, T5, T6>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4,
            Action<IAvroDecoder> skipper5,
            Action<IAvroDecoder> skipper6
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
                case 5:
                    skipper6.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 5);
                    throw ex;
            }
        }

        public void SkipUnion<T1, T2, T3, T4, T5, T6, T7>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4,
            Action<IAvroDecoder> skipper5,
            Action<IAvroDecoder> skipper6,
            Action<IAvroDecoder> skipper7
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
                case 5:
                    skipper6.Invoke(this);
                    break;
                case 6:
                    skipper7.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 6);
                    throw ex;
            }
        }

        public void SkipUnion<T1, T2, T3, T4, T5, T6, T7, T8>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4,
            Action<IAvroDecoder> skipper5,
            Action<IAvroDecoder> skipper6,
            Action<IAvroDecoder> skipper7,
            Action<IAvroDecoder> skipper8
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
                case 5:
                    skipper6.Invoke(this);
                    break;
                case 6:
                    skipper7.Invoke(this);
                    break;
                case 7:
                    skipper8.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 7);
                    throw ex;
            }
        }

        public void SkipUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4,
            Action<IAvroDecoder> skipper5,
            Action<IAvroDecoder> skipper6,
            Action<IAvroDecoder> skipper7,
            Action<IAvroDecoder> skipper8,
            Action<IAvroDecoder> skipper9
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
                case 5:
                    skipper6.Invoke(this);
                    break;
                case 6:
                    skipper7.Invoke(this);
                    break;
                case 7:
                    skipper8.Invoke(this);
                    break;
                case 8:
                    skipper8.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 8);
                    throw ex;
            }
        }

        private static IndexOutOfRangeException UnionIndexException(long index, long range)
        {
            return new IndexOutOfRangeException($"Union Index out of range: '{index}'. Valid range [{0}:{range}]");
        }

        public void Dispose() { }

        private int Advance(bool quote, out string value)
        {
            if (_index >= _actions.Length)
                _index = 0;

            return _actions[_index++].Invoke(_reader, quote, out value);
        }
    }
}
