using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Avro.IO
{
    public class JsonDecoder : IDecoder
    {
        private JsonReader _stream;

        public JsonDecoder(JsonReader stream)
        {
            _stream = stream;
        }

        public IList<T> ReadArray<T>(Func<IDecoder, T> itemsReader)
        {
            throw new NotImplementedException();
        }

        public bool ReadBoolean()
        {
            throw new NotImplementedException();
        }

        public byte[] ReadBytes()
        {
            throw new NotImplementedException();
        }

        public byte[] ReadBytes(byte[] bytes)
        {
            throw new NotImplementedException();
        }

        public DateTime ReadDate()
        {
            throw new NotImplementedException();
        }

        public decimal ReadDecimal(int scale)
        {
            throw new NotImplementedException();
        }

        public decimal ReadDecimal(int scale, int len)
        {
            throw new NotImplementedException();
        }

        public double ReadDouble()
        {
            throw new NotImplementedException();
        }

        public Tuple<int, int, int> ReadDuration()
        {
            throw new NotImplementedException();
        }

        public byte[] ReadFixed(byte[] bytes)
        {
            throw new NotImplementedException();
        }

        public byte[] ReadFixed(int len)
        {
            throw new NotImplementedException();
        }

        public float ReadFloat()
        {
            throw new NotImplementedException();
        }

        public int ReadInt()
        {
            throw new NotImplementedException();
        }

        public long ReadLong()
        {
            throw new NotImplementedException();
        }

        public IDictionary<string, T> ReadMap<T>(Func<IDecoder, T> valuesReader)
        {
            throw new NotImplementedException();
        }

        public T ReadNullableObject<T>(Func<IDecoder, T> reader, long nullIndex) where T : class
        {
            throw new NotImplementedException();
        }

        public T? ReadNullableValue<T>(Func<IDecoder, T> reader, long nullIndex) where T : struct
        {
            throw new NotImplementedException();
        }

        public string ReadString()
        {
            throw new NotImplementedException();
        }

        public TimeSpan ReadTimeMS()
        {
            throw new NotImplementedException();
        }

        public TimeSpan ReadTimeNS()
        {
            throw new NotImplementedException();
        }

        public DateTime ReadTimestampMS()
        {
            throw new NotImplementedException();
        }

        public DateTime ReadTimestampNS()
        {
            throw new NotImplementedException();
        }

        public DateTime ReadTimestampUS()
        {
            throw new NotImplementedException();
        }

        public TimeSpan ReadTimeUS()
        {
            throw new NotImplementedException();
        }

        public object ReadUnion(Func<IDecoder, object>[] readers)
        {
            throw new NotImplementedException();
        }

        public Guid ReadUuid()
        {
            throw new NotImplementedException();
        }

        public void SkipArray(Action<IDecoder> itemsSkipper)
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

        public void SkipFixed(long len)
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

        public void SkipMap(Action<IDecoder> valuesSkipper)
        {
            throw new NotImplementedException();
        }

        public void SkipNull()
        {
            throw new NotImplementedException();
        }

        public void SkipNullable(Action<IDecoder> skipper, long nullIndex)
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

        public void SkipUnion(Action<IDecoder>[] skippers)
        {
            throw new NotImplementedException();
        }

        public void SkipUuid()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            _stream = null;
        }

        public object ReadNull()
        {
            throw new NotImplementedException();
        }
    }
}
