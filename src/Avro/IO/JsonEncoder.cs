using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Avro.IO
{
    public class JsonEncoder : IEncoder
    {
        private JsonWriter _stream;

        public JsonEncoder(JsonWriter stream)
        {
            _stream = stream;
        }

        public void WriteArray<T>(IList<T> items, Action<IEncoder, T> itemsWriter)
        {
            throw new NotImplementedException();
        }

        public void WriteBoolean(bool value)
        {
            throw new NotImplementedException();
        }

        public void WriteBytes(byte[] value)
        {
            throw new NotImplementedException();
        }

        public void WriteDate(DateTime value)
        {
            throw new NotImplementedException();
        }

        public void WriteDecimal(decimal value)
        {
            throw new NotImplementedException();
        }

        public void WriteDecimal(decimal value, int len)
        {
            throw new NotImplementedException();
        }

        public void WriteDouble(double value)
        {
            throw new NotImplementedException();
        }

        public void WriteDuration(Tuple<int, int, int> value)
        {
            throw new NotImplementedException();
        }

        public void WriteFixed(byte[] value)
        {
            throw new NotImplementedException();
        }

        public void WriteFloat(float value)
        {
            throw new NotImplementedException();
        }

        public void WriteInt(int value)
        {
            throw new NotImplementedException();
        }

        public void WriteLong(long value)
        {
            throw new NotImplementedException();
        }

        public void WriteMap<T>(IDictionary<string, T> keyValues, Action<IEncoder, T> valuesWriter)
        {
            throw new NotImplementedException();
        }

        public void WriteString(string value)
        {
            throw new NotImplementedException();
        }

        public void WriteTimeMS(TimeSpan value)
        {
            throw new NotImplementedException();
        }

        public void WriteTimeNS(TimeSpan value)
        {
            throw new NotImplementedException();
        }

        public void WriteTimestampMS(DateTime value)
        {
            throw new NotImplementedException();
        }

        public void WriteTimestampNS(DateTime value)
        {
            throw new NotImplementedException();
        }

        public void WriteTimestampUS(DateTime value)
        {
            throw new NotImplementedException();
        }

        public void WriteTimeUS(TimeSpan value)
        {
            throw new NotImplementedException();
        }

        public void WriteUuid(Guid value)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            _stream = null;
        }

        public void WriteNull()
        {
            throw new NotImplementedException();
        }
    }
}
