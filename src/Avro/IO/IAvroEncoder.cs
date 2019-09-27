using Avro.Types;
using System;
using System.Collections.Generic;

namespace Avro.IO
{
    public interface IAvroEncoder : IDisposable
    {
        void WriteArray<T>(IList<T> items, Action<IAvroEncoder, T> itemsWriter);
        void WriteArrayBlock<T>(IList<T> items, Action<IAvroEncoder, T> itemsWriter);
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
        void WriteMap<T>(IDictionary<string, T> keyValues, Action<IAvroEncoder, T> valuesWriter);
        void WriteMapBlock<T>(IDictionary<string, T> keyValues, Action<IAvroEncoder, T> valuesWriter);
        void WriteNull(AvroNull value);
        void WriteNullableObject<T>(T value, Action<IAvroEncoder, T> valueWriter, long nullIndex) where T : class;
        void WriteNullableValue<T>(T? value, Action<IAvroEncoder, T> valueWriter, long nullIndex) where T : struct;
        void WriteString(string value);
        void WriteTimeMS(TimeSpan value);
        void WriteTimeNS(TimeSpan value);
        void WriteTimestampMS(DateTime value);
        void WriteTimestampNS(DateTime value);
        void WriteTimestampUS(DateTime value);
        void WriteTimeUS(TimeSpan value);
        void WriteUuid(Guid value);
        void WriteUnion<T1, T2>(
            AvroUnion<T1, T2> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2
        );
        void WriteUnion<T1, T2, T3>(
            AvroUnion<T1, T2, T3> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3
        );
        void WriteUnion<T1, T2, T3, T4>(
            AvroUnion<T1, T2, T3, T4> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4
        );
        void WriteUnion<T1, T2, T3, T4, T5>(
            AvroUnion<T1, T2, T3, T4, T5> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4,
            Action<IAvroEncoder, T5> valueWriter5
        );
        void WriteUnion<T1, T2, T3, T4, T5, T6>(
            AvroUnion<T1, T2, T3, T4, T5, T6> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4,
            Action<IAvroEncoder, T5> valueWriter5,
            Action<IAvroEncoder, T6> valueWriter6
        );
        void WriteUnion<T1, T2, T3, T4, T5, T6, T7>(
            AvroUnion<T1, T2, T3, T4, T5, T6, T7> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4,
            Action<IAvroEncoder, T5> valueWriter5,
            Action<IAvroEncoder, T6> valueWriter6,
            Action<IAvroEncoder, T7> valueWriter7
        );
        void WriteUnion<T1, T2, T3, T4, T5, T6, T7, T8>(
            AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4,
            Action<IAvroEncoder, T5> valueWriter5,
            Action<IAvroEncoder, T6> valueWriter6,
            Action<IAvroEncoder, T7> valueWriter7,
            Action<IAvroEncoder, T8> valueWriter8
        );
        void WriteUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(
            AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4,
            Action<IAvroEncoder, T5> valueWriter5,
            Action<IAvroEncoder, T6> valueWriter6,
            Action<IAvroEncoder, T7> valueWriter7,
            Action<IAvroEncoder, T8> valueWriter8,
            Action<IAvroEncoder, T9> valueWriter9
        );
    }
}
