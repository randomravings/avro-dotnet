using Avro.Types;
using System;
using System.Collections.Generic;

namespace Avro.IO
{
    public interface IAvroDecoder : IDisposable
    {
        IList<T> ReadArray<T>(Func<IAvroDecoder, T> itemsReader);
        bool ReadArrayBlock<T>(Func<IAvroDecoder, T> itemsReader, out IList<T> array);
        bool ReadBoolean();
        byte[] ReadBytes();
        DateTime ReadDate();
        decimal ReadDecimal(int scale);
        decimal ReadDecimal(int scale, int len);
        double ReadDouble();
        AvroDuration ReadDuration();
        T ReadEnum<T>() where T : struct, Enum;
        T ReadEnum<T>(T value) where T : notnull, IAvroEnum;
        byte[] ReadFixed(int len);
        byte[] ReadFixed(byte[] bytes);
        T ReadFixed<T>(T bytes) where T : notnull, IAvroFixed;
        float ReadFloat();
        int ReadInt();
        long ReadLong();
        IDictionary<string, T> ReadMap<T>(Func<IAvroDecoder, T> valuesReader);
        bool ReadMapBlock<T>(Func<IAvroDecoder, T> valuesReader, out IDictionary<string, T> map);
        AvroNull ReadNull();
        T? ReadNullableObject<T>(Func<IAvroDecoder, T> reader, long nullIndex) where T : class;
        T? ReadNullableValue<T>(Func<IAvroDecoder, T> reader, long nullIndex) where T : struct;
        string ReadString();
        TimeSpan ReadTimeMS();
        TimeSpan ReadTimeNS();
        DateTime ReadTimestampMS();
        DateTime ReadTimestampNS();
        DateTime ReadTimestampUS();
        TimeSpan ReadTimeUS();
        Guid ReadUuid();
        AvroUnion<T1> ReadUnion<T1>(
            Func<IAvroDecoder, T1> valuesReader1
        )
        where T1 : notnull;
        AvroUnion<T1, T2> ReadUnion<T1, T2>(
            Func<IAvroDecoder, T1> valuesReader1,
            Func<IAvroDecoder, T2> valuesReader2
        )
            where T1 : notnull
            where T2 : notnull
        ;
        AvroUnion<T1, T2, T3> ReadUnion<T1, T2, T3>(
            Func<IAvroDecoder, T1> valuesReader1,
            Func<IAvroDecoder, T2> valuesReader2,
            Func<IAvroDecoder, T3> valuesReader3
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
        ;
        AvroUnion<T1, T2, T3, T4> ReadUnion<T1, T2, T3, T4>(
            Func<IAvroDecoder, T1> valuesReader1,
            Func<IAvroDecoder, T2> valuesReader2,
            Func<IAvroDecoder, T3> valuesReader3,
            Func<IAvroDecoder, T4> valuesReader4
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
            where T4 : notnull
        ;
        AvroUnion<T1, T2, T3, T4, T5> ReadUnion<T1, T2, T3, T4, T5>(
            Func<IAvroDecoder, T1> valuesReader1,
            Func<IAvroDecoder, T2> valuesReader2,
            Func<IAvroDecoder, T3> valuesReader3,
            Func<IAvroDecoder, T4> valuesReader4,
            Func<IAvroDecoder, T5> valuesReader5
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
            where T4 : notnull
            where T5 : notnull
        ;
        void SkipArray(Action<IAvroDecoder> itemsSkipper);
        void SkipBoolean();
        void SkipBytes();
        void SkipDate();
        void SkipDecimal();
        void SkipDecimal(int len);
        void SkipDouble();
        void SkipDuration();
        void SkipEnum();
        void SkipFixed(int len);
        void SkipFloat();
        void SkipInt();
        void SkipLong();
        void SkipMap(Action<IAvroDecoder> valuesSkipper);
        void SkipNull();
        void SkipNullable(Action<IAvroDecoder> skipper, long nullIndex);
        void SkipString();
        void SkipTimeMS();
        void SkipTimeNS();
        void SkipTimestampMS();
        void SkipTimestampNS();
        void SkipTimestampUS();
        void SkipTimeUS();
        void SkipUuid();
        void SkipUnion<T1>(
            Action<IAvroDecoder> skipper1
        );
        void SkipUnion<T1, T2>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2
        );
        void SkipUnion<T1, T2, T3>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3
        );
        void SkipUnion<T1, T2, T3, T4>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4
        );
        void SkipUnion<T1, T2, T3, T4, T5>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4,
            Action<IAvroDecoder> skipper5
        );
    }
}
