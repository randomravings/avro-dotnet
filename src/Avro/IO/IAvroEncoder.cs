using Avro.Types;
using System;
using System.Collections.Generic;

namespace Avro.IO
{
    public interface IAvroEncoder : IDisposable
    {
        void WriteArray<T>(IList<T> items, Action<IAvroEncoder, T> itemsWriter);
        void WriteArray<A, T>(A items, Action<IAvroEncoder, T> itemsWriter) where A : notnull, IList<T>;
        void WriteArrayStart();
        void WriteArrayBlock<T>(IList<T> items, Action<IAvroEncoder, T> itemsWriter);
        void WriteArrayBlock<A, T>(A items, Action<IAvroEncoder, T> itemsWriter) where A : notnull, IList<T>;
        void WriteArrayEnd();
        void WriteBoolean(bool value);
        void WriteBytes(byte[] value);
        void WriteDate(DateTime value);
        void WriteDecimal(decimal value, int scale);
        void WriteDecimal(decimal value, int scale, int len);
        void WriteDouble(double value);
        void WriteEnum<T>(T value) where T : struct, Enum;
        void WriteEnum(IAvroEnum value);
        void WriteDuration(AvroDuration value);
        void WriteFixed<T>(T value) where T : notnull, IAvroFixed;
        void WriteFixed(byte[] value);
        void WriteFloat(float value);
        void WriteInt(int value);
        void WriteLong(long value);
        void WriteMap<T>(IDictionary<string, T> keyValues, Action<IAvroEncoder, T> valuesWriter);
        void WriteMap<M, T>(M keyValues, Action<IAvroEncoder, T> valuesWriter) where M : notnull, IDictionary<string, T>;
        void WriteMapStart();
        void WriteMapBlock<T>(IDictionary<string, T> keyValues, Action<IAvroEncoder, T> valuesWriter);
        void WriteMapBlock<M, T>(M keyValues, Action<IAvroEncoder, T> valuesWriter) where M : notnull, IDictionary<string, T>;
        void WriteMapEnd();
        void WriteNull(AvroNull value);
        void WriteNullableObject<T>(T? value, Action<IAvroEncoder, T> valueWriter, long nullIndex) where T : class;
        void WriteNullableValue<T>(T? value, Action<IAvroEncoder, T> valueWriter, long nullIndex) where T : struct;
        void WriteString(string value);
        void WriteTimeMS(TimeSpan value);
        void WriteTimeNS(TimeSpan value);
        void WriteTimestampMS(DateTime value);
        void WriteTimestampNS(DateTime value);
        void WriteTimestampUS(DateTime value);
        void WriteTimeUS(TimeSpan value);
        void WriteUuid(Guid value);
        void WriteUnion<T1>(
            AvroUnion<T1> value,
            Action<IAvroEncoder, T1> valueWriter1
        )
            where T1 : notnull;
        void WriteUnion<T1, T2>(
            AvroUnion<T1, T2> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2
        )
            where T1 : notnull
            where T2 : notnull
        ;
        void WriteUnion<T1, T2, T3>(
            AvroUnion<T1, T2, T3> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
        ;
        void WriteUnion<T1, T2, T3, T4>(
            AvroUnion<T1, T2, T3, T4> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
            where T4 : notnull
        ;
        void WriteUnion<T1, T2, T3, T4, T5>(
            AvroUnion<T1, T2, T3, T4, T5> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4,
            Action<IAvroEncoder, T5> valueWriter5
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
            where T4 : notnull
            where T5 : notnull
        ;
    }
}
