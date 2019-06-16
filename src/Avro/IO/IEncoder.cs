using System;
using System.Collections.Generic;

namespace Avro.IO
{   
    public interface IEncoder : IDisposable
    {
        void WriteArray<T>(IList<T> items, Action<IEncoder, T> itemsWriter);
        void WriteBoolean(bool value);
        void WriteBytes(byte[] value);
        void WriteDate(DateTime value);
        void WriteDecimal(decimal value);
        void WriteDecimal(decimal value, int len);
        void WriteDouble(double value);
        void WriteDuration(Tuple<int, int, int> value);
        void WriteFixed(byte[] value);
        void WriteFloat(float value);
        void WriteInt(int value);
        void WriteLong(long value);
        void WriteMap<T>(IDictionary<string, T> keyValues, Action<IEncoder, T> valuesWriter);
        void WriteNull();
        void WriteString(string value);
        void WriteTimeMS(TimeSpan value);
        void WriteTimeNS(TimeSpan value);
        void WriteTimestampMS(DateTime value);
        void WriteTimestampNS(DateTime value);
        void WriteTimestampUS(DateTime value);
        void WriteTimeUS(TimeSpan value);
        void WriteUuid(Guid value);
    }
}
