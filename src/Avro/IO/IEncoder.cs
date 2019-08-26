using Avro.Types;
using System;
using System.Collections.Generic;

namespace Avro.IO
{
    public interface IEncoder : IDisposable
    {
        void WriteArray<T>(IList<T> items, Action<IEncoder, T> itemsWriter);
        void WriteArrayBlock<T>(IList<T> items, Action<IEncoder, T> itemsWriter);
        void WriteBoolean(bool value);
        void WriteBytes(byte[] value);
        void WriteDate(DateTime value);
        void WriteDecimal(decimal value, int scale);
        void WriteDecimal(decimal value, int scale, int len);
        void WriteDouble(double value);
        void WriteDuration(AvroDuration value);
        void WriteFixed(byte[] value);
        void WriteFloat(float value);
        void WriteInt(int value);
        void WriteLong(long value);
        void WriteMap<T>(IDictionary<string, T> keyValues, Action<IEncoder, T> valuesWriter);
        void WriteMapBlock<T>(IDictionary<string, T> keyValues, Action<IEncoder, T> valuesWriter);
        void WriteNull();
        void WriteNullableObject<T>(T value, Action<IEncoder, T> valueWriter, long nullIndex) where T : class;
        void WriteNullableValue<T>(T? value, Action<IEncoder, T> valueWriter, long nullIndex) where T : struct;
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
