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
        byte[] ReadFixed(byte[] bytes);
        byte[] ReadFixed(int len);
        float ReadFloat();
        int ReadInt();
        long ReadLong();
        IDictionary<string, T> ReadMap<T>(Func<IAvroDecoder, T> valuesReader);
        bool ReadMapBlock<T>(Func<IAvroDecoder, T> valuesReader, out IDictionary<string, T> map);
        AvroNull ReadNull();
        T ReadNullableObject<T>(Func<IAvroDecoder, T> reader, long nullIndex) where T : class;
        T? ReadNullableValue<T>(Func<IAvroDecoder, T> reader, long nullIndex) where T : struct;
        string ReadString();
        TimeSpan ReadTimeMS();
        TimeSpan ReadTimeNS();
        DateTime ReadTimestampMS();
        DateTime ReadTimestampNS();
        DateTime ReadTimestampUS();
        TimeSpan ReadTimeUS();
        Guid ReadUuid();
        AvroUnion<T1, T2> ReadUnion<T1, T2>(
            Func<IAvroDecoder, T1> valuesReader1,
            Func<IAvroDecoder, T2> valuesReader2
        );
        AvroUnion<T1, T2, T3> ReadUnion<T1, T2, T3>(
            Func<IAvroDecoder, T1> valuesReader1,
            Func<IAvroDecoder, T2> valuesReader2,
            Func<IAvroDecoder, T3> valuesReader3
        );
        AvroUnion<T1, T2, T3, T4> ReadUnion<T1, T2, T3, T4>(
            Func<IAvroDecoder, T1> valuesReader1,
            Func<IAvroDecoder, T2> valuesReader2,
            Func<IAvroDecoder, T3> valuesReader3,
            Func<IAvroDecoder, T4> valuesReader4
        );
        AvroUnion<T1, T2, T3, T4, T5> ReadUnion<T1, T2, T3, T4, T5>(
            Func<IAvroDecoder, T1> valuesReader1,
            Func<IAvroDecoder, T2> valuesReader2,
            Func<IAvroDecoder, T3> valuesReader3,
            Func<IAvroDecoder, T4> valuesReader4,
            Func<IAvroDecoder, T5> valuesReader5
        );
        AvroUnion<T1, T2, T3, T4, T5, T6> ReadUnion<T1, T2, T3, T4, T5, T6>(
            Func<IAvroDecoder, T1> valuesReader1,
            Func<IAvroDecoder, T2> valuesReader2,
            Func<IAvroDecoder, T3> valuesReader3,
            Func<IAvroDecoder, T4> valuesReader4,
            Func<IAvroDecoder, T5> valuesReader5,
            Func<IAvroDecoder, T6> valuesReader6
        );
        AvroUnion<T1, T2, T3, T4, T5, T6, T7> ReadUnion<T1, T2, T3, T4, T5, T6, T7>(
            Func<IAvroDecoder, T1> valuesReader1,
            Func<IAvroDecoder, T2> valuesReader2,
            Func<IAvroDecoder, T3> valuesReader3,
            Func<IAvroDecoder, T4> valuesReader4,
            Func<IAvroDecoder, T5> valuesReader5,
            Func<IAvroDecoder, T6> valuesReader6,
            Func<IAvroDecoder, T7> valuesReader7
        );
        AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> ReadUnion<T1, T2, T3, T4, T5, T6, T7, T8>(
            Func<IAvroDecoder, T1> valuesReader1,
            Func<IAvroDecoder, T2> valuesReader2,
            Func<IAvroDecoder, T3> valuesReader3,
            Func<IAvroDecoder, T4> valuesReader4,
            Func<IAvroDecoder, T5> valuesReader5,
            Func<IAvroDecoder, T6> valuesReader6,
            Func<IAvroDecoder, T7> valuesReader7,
            Func<IAvroDecoder, T8> valuesReader8
        );
        AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> ReadUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(
            Func<IAvroDecoder, T1> valuesReader1,
            Func<IAvroDecoder, T2> valuesReader2,
            Func<IAvroDecoder, T3> valuesReader3,
            Func<IAvroDecoder, T4> valuesReader4,
            Func<IAvroDecoder, T5> valuesReader5,
            Func<IAvroDecoder, T6> valuesReader6,
            Func<IAvroDecoder, T7> valuesReader7,
            Func<IAvroDecoder, T8> valuesReader8,
            Func<IAvroDecoder, T9> valuesReader9
        );
        void SkipArray(Action<IAvroDecoder> itemsSkipper);
        void SkipBoolean();
        void SkipBytes();
        void SkipDate();
        void SkipDecimal();
        void SkipDecimal(int len);
        void SkipDouble();
        void SkipDuration();
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
        void SkipUnion<T1, T2, T3, T4, T5, T6>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4,
            Action<IAvroDecoder> skipper5,
            Action<IAvroDecoder> skipper6
        );
        void SkipUnion<T1, T2, T3, T4, T5, T6, T7>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4,
            Action<IAvroDecoder> skipper5,
            Action<IAvroDecoder> skipper6,
            Action<IAvroDecoder> skipper7
        );
        void SkipUnion<T1, T2, T3, T4, T5, T6, T7, T8>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4,
            Action<IAvroDecoder> skipper5,
            Action<IAvroDecoder> skipper6,
            Action<IAvroDecoder> skipper7,
            Action<IAvroDecoder> skipper8
        );
        void SkipUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4,
            Action<IAvroDecoder> skipper5,
            Action<IAvroDecoder> skipper6,
            Action<IAvroDecoder> skipper7,
            Action<IAvroDecoder> skipper8,
            Action<IAvroDecoder> skipper9
        );
    }
}
