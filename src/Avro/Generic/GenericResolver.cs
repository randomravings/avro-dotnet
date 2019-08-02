using Avro.IO;
using Avro.Schemas;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Generic
{
    public static class GenericResolver
    {
        #region Static Encoders
        private static readonly dynamic ENCODE_NULL =
            new Action<IEncoder, object>((s, v) => { });

        private static readonly dynamic ENCODE_BOOLEAN =
            new Action<IEncoder, object>((s, v) => s.WriteBoolean((bool)v));

        private static readonly dynamic ENCODE_INT =
            new Action<IEncoder, object>((s, v) => s.WriteInt((int)v));

        private static readonly dynamic ENCODE_LONG =
            new Action<IEncoder, object>((s, v) => s.WriteLong((long)v));

        private static readonly dynamic ENCODE_FLOAT =
            new Action<IEncoder, object>((s, v) => s.WriteFloat((float)v));

        private static readonly dynamic ENCODE_DOUBLE =
            new Action<IEncoder, object>((s, v) => s.WriteDouble((double)v));

        private static readonly dynamic ENCODE_BYTES =
            new Action<IEncoder, object>((s, v) => s.WriteBytes((byte[])v));

        private static readonly dynamic ENCODE_STRING =
            new Action<IEncoder, object>((s, v) => s.WriteString((string)v));

        private static readonly dynamic ENCODE_UUID =
            new Action<IEncoder, object>((s, v) => s.WriteUuid((Guid)v));

        private static readonly dynamic ENCODE_DATE =
            new Action<IEncoder, object>((s, v) => s.WriteDate((DateTime)v));

        private static readonly dynamic ENCODE_TIME_MS =
            new Action<IEncoder, object>((s, v) => s.WriteTimeMS((TimeSpan)v));

        private static readonly dynamic ENCODE_TIME_US =
            new Action<IEncoder, object>((s, v) => s.WriteTimeUS((TimeSpan)v));

        private static readonly dynamic ENCODE_TIME_NS =
            new Action<IEncoder, object>((s, v) => s.WriteTimeNS((TimeSpan)v));

        private static readonly dynamic ENCODE_TIMESTAMP_MS =
            new Action<IEncoder, object>((s, v) => s.WriteTimestampMS((DateTime)v));

        private static readonly dynamic ENCODE_TIMESTAMP_US =
            new Action<IEncoder, object>((s, v) => s.WriteTimestampUS((DateTime)v));

        private static readonly dynamic ENCODE_TIMESTAMP_NS =
            new Action<IEncoder, object>((s, v) => s.WriteTimestampNS((DateTime)v));

        private static readonly dynamic ENCODE_DURATION =
            new Action<IEncoder, object>((s, v) => s.WriteDuration((ValueTuple<int, int, int>)v));
        #endregion

        #region Static Decoders
        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_NULL =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => null), new Action<IDecoder>(s => { }));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_BOOLEAN =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadBoolean()), new Action<IDecoder>(s => s.SkipBoolean()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_INT =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadInt()), new Action<IDecoder>(s => s.SkipInt()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_LONG =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadLong()), new Action<IDecoder>(s => s.SkipLong()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_LONG_FROM_INT =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadInt()), new Action<IDecoder>(s => s.SkipInt()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_FLOAT =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadFloat()), new Action<IDecoder>(s => s.SkipFloat()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_FLOAT_FROM_LONG =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadLong()), new Action<IDecoder>(s => s.SkipLong()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_FLOAT_FROM_INT =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadInt()), new Action<IDecoder>(s => s.SkipInt()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_DOUBLE =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadDouble()), new Action<IDecoder>(s => s.SkipDouble()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_DOUBLE_FROM_FLOAT =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadFloat()), new Action<IDecoder>(s => s.SkipFloat()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_DOUBLE_FROM_LONG =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadLong()), new Action<IDecoder>(s => s.SkipLong()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_DOUBLE_FROM_INT =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadInt()), new Action<IDecoder>(s => s.SkipInt()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_BYTES =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadBytes()), new Action<IDecoder>(s => s.SkipBytes()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_BYTES_FROM_STRING =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadBytes()), new Action<IDecoder>(s => s.SkipBytes()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_STRING =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadString()), new Action<IDecoder>(s => s.SkipString()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_STRING_FROM_BYTES =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadString()), new Action<IDecoder>(s => s.SkipString()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_UUID =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadUuid()), new Action<IDecoder>(s => s.SkipUuid()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_DATE =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadDate()), new Action<IDecoder>(s => s.SkipDate()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_TIME_MS =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadTimeMS()), new Action<IDecoder>(s => s.SkipTimeMS()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_TIME_US =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadTimeUS()), new Action<IDecoder>(s => s.SkipTimeUS()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_TIME_US_FROM_MS =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadTimeMS()), new Action<IDecoder>(s => s.SkipTimeMS()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_TIME_NS =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadTimeNS()), new Action<IDecoder>(s => s.SkipTimeNS()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_TIME_NS_FROM_US =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadTimeUS()), new Action<IDecoder>(s => s.SkipTimeUS()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_TIME_NS_FROM_MS =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadTimeMS()), new Action<IDecoder>(s => s.SkipTimeMS()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_TIMESTAMP_MS =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadTimestampMS()), new Action<IDecoder>(s => s.SkipTimestampMS()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_TIMESTAMP_US =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadTimestampUS()), new Action<IDecoder>(s => s.SkipTimestampUS()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_TIMESTAMP_US_FROM_MS =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadTimestampMS()), new Action<IDecoder>(s => s.SkipTimestampMS()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_TIMESTAMP_NS =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadTimestampNS()), new Action<IDecoder>(s => s.SkipTimestampNS()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_TIMESTAMP_NS_FROM_US =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadTimestampUS()), new Action<IDecoder>(s => s.SkipTimestampUS()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_TIMESTAMP_NS_FROM_MS =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadTimestampMS()), new Action<IDecoder>(s => s.SkipTimestampMS()));

        private static readonly Tuple<Func<IDecoder, object>, Action<IDecoder>> DECODE_DURATION =
            new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadDuration()), new Action<IDecoder>(s => s.SkipDuration()));
        #endregion

        public static Action<IEncoder, object> ResolveWriter(Schema writerSchema)
        {
            switch (writerSchema)
            {
                case NullSchema r:
                    return ENCODE_NULL;
                case BooleanSchema r:
                    return ENCODE_BOOLEAN;
                case IntSchema r:
                    return ENCODE_INT;
                case LongSchema r:
                    return ENCODE_LONG;
                case FloatSchema r:
                    return ENCODE_FLOAT;
                case DoubleSchema r:
                    return ENCODE_DOUBLE;
                case BytesSchema r:
                    return ENCODE_BYTES;
                case StringSchema r:
                    return ENCODE_STRING;
                case UuidSchema r:
                    return ENCODE_UUID;
                case DateSchema r:
                    return ENCODE_DATE;
                case TimeMillisSchema r:
                    return ENCODE_TIME_MS;
                case TimeMicrosSchema r:
                    return ENCODE_TIME_US;
                case TimeNanosSchema r:
                    return ENCODE_TIME_NS;
                case TimestampMillisSchema r:
                    return ENCODE_TIMESTAMP_MS;
                case TimestampMicrosSchema r:
                    return ENCODE_TIMESTAMP_US;
                case TimestampNanosSchema r:
                    return ENCODE_TIMESTAMP_NS;
                case DurationSchema r:
                    return ENCODE_DURATION;
                case DecimalSchema r:
                    var decimalWriter = writerSchema as DecimalSchema;
                    switch (decimalWriter.Type)
                    {
                        case BytesSchema t:
                            return new Action<IEncoder, object>((s, v) => s.WriteDecimal((decimal)v, r.Scale));
                        case FixedSchema t:
                            var decimalLength = (decimalWriter.Type as FixedSchema).Size;
                            return new Action<IEncoder, object>((s, v) => s.WriteDecimal((decimal)v, r.Scale, decimalLength));
                    }
                    break;

                case EnumSchema r:
                    var symbolMap = new SortedList<string, int>();
                    for (int i = 0; i < r.Symbols.Count; i++)
                        symbolMap.Add(r.Symbols[i], i);
                    return new Action<IEncoder, object>((s, v) => s.WriteInt(symbolMap[(string)v]));

                case FixedSchema r:
                    return new Action<IEncoder, object>((s, v) => s.WriteFixed((byte[])v));

                case RecordSchema r:
                    var fieldWriters = new Action<IEncoder, object>[r.Count];
                    for (int i = 0; i < fieldWriters.Length; i++)
                        fieldWriters[i] = ResolveWriter(r.ElementAt(i).Type);
                    return new Action<IEncoder, object>(
                        (s, v) =>
                        {
                            var record = v as GenericRecord;
                            for (int i = 0; i < fieldWriters.Length; i++)
                                fieldWriters[i].Invoke(s, record[i]);
                        });
            }
            throw new AvroException($"Unable to resolve writer for: '{writerSchema}'.");
        }

        public static Tuple<Func<IDecoder, object>, Action<IDecoder>> ResolveReader(Schema readerSchema, Schema writerSchema)
        {
            switch (readerSchema)
            {
                case NullSchema r when writerSchema is NullSchema:
                    return DECODE_NULL;
                case BooleanSchema r when writerSchema is BooleanSchema:
                    return DECODE_BOOLEAN;
                case IntSchema r when writerSchema is IntSchema:
                    return DECODE_INT;
                case LongSchema r when writerSchema is LongSchema:
                    return DECODE_LONG;
                case LongSchema r when writerSchema is IntSchema:
                    return DECODE_LONG_FROM_INT;
                case FloatSchema r when writerSchema is FloatSchema:
                    return DECODE_FLOAT;
                case FloatSchema r when writerSchema is LongSchema:
                    return DECODE_FLOAT_FROM_LONG;
                case FloatSchema r when writerSchema is IntSchema:
                    return DECODE_FLOAT_FROM_INT;
                case DoubleSchema r when writerSchema is FloatSchema:
                    return DECODE_DOUBLE;
                case DoubleSchema r when writerSchema is FloatSchema:
                    return DECODE_DOUBLE_FROM_FLOAT;
                case DoubleSchema r when writerSchema is LongSchema:
                    return DECODE_DOUBLE_FROM_LONG;
                case DoubleSchema r when writerSchema is IntSchema:
                    return DECODE_DOUBLE_FROM_INT;
                case BytesSchema r when writerSchema is BytesSchema:
                    return DECODE_BYTES;
                case BytesSchema r when writerSchema is StringSchema:
                    return DECODE_BYTES_FROM_STRING;
                case StringSchema r when writerSchema is StringSchema:
                    return DECODE_STRING;
                case StringSchema r when writerSchema is BytesSchema:
                    return DECODE_STRING_FROM_BYTES;
                case UuidSchema r when writerSchema is UuidSchema:
                    return DECODE_UUID;
                case DateSchema r when writerSchema is DateSchema:
                    return DECODE_DATE;
                case TimeMillisSchema r when writerSchema is TimeMillisSchema:
                    return DECODE_TIME_MS;
                case TimeMicrosSchema r when writerSchema is TimeMicrosSchema:
                    return DECODE_TIME_US;
                case TimeMicrosSchema r when writerSchema is TimeMillisSchema:
                    return DECODE_TIME_US_FROM_MS;
                case TimeNanosSchema r when writerSchema is TimeNanosSchema:
                    return DECODE_TIME_NS;
                case TimeNanosSchema r when writerSchema is TimeMicrosSchema:
                    return DECODE_TIME_NS_FROM_US;
                case TimeNanosSchema r when writerSchema is TimeMillisSchema:
                    return DECODE_TIME_NS_FROM_MS;
                case TimestampMillisSchema r when writerSchema is TimestampMillisSchema:
                    return DECODE_TIMESTAMP_MS;
                case TimestampMicrosSchema r when writerSchema is TimestampMicrosSchema:
                    return DECODE_TIMESTAMP_US;
                case TimestampMicrosSchema r when writerSchema is TimestampMillisSchema:
                    return DECODE_TIMESTAMP_US_FROM_MS;
                case TimestampNanosSchema r when writerSchema is TimestampNanosSchema:
                    return DECODE_TIMESTAMP_NS;
                case TimestampNanosSchema r when writerSchema is TimestampMicrosSchema:
                    return DECODE_TIMESTAMP_NS_FROM_US;
                case TimestampNanosSchema r when writerSchema is TimestampMillisSchema:
                    return DECODE_TIMESTAMP_NS_FROM_MS;
                case DurationSchema r when writerSchema is DurationSchema:
                    return DECODE_DURATION;
                case DecimalSchema r when writerSchema is DecimalSchema:
                    var decimalWriter = writerSchema as DecimalSchema;
                    if (!decimalWriter.Equals(r))
                        break;
                    switch (decimalWriter.Type)
                    {
                        case BytesSchema t:
                            return new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadDecimal(r.Scale)), new Action<IDecoder>(s => s.SkipDecimal()));
                        case FixedSchema t:
                            var decimalLength = (decimalWriter.Type as FixedSchema).Size;
                            return new Tuple<Func<IDecoder, object>, Action<IDecoder>>(new Func<IDecoder, object>(s => s.ReadDecimal(r.Scale, decimalLength)), new Action<IDecoder>(s => s.SkipDecimal(decimalLength)));
                    }
                    break;
            }
            throw new AvroException($"Unable to resolve reader for: '{readerSchema}' using writer: '{writerSchema}'.");
        }
    }
}
