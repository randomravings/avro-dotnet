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
    }
}
