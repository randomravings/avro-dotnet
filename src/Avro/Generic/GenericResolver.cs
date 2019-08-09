using Avro.IO;
using Avro.Schemas;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Generic
{
    public static class GenericResolver
    {
        public static Action<IEncoder, object> ResolveWriter<T>(Schema writerSchema)
        {
            var writer = ResolveWriter(writerSchema, typeof(T));
            if (writer == null)
                throw new AvroException($"Unable to resolve writer for: '{writerSchema}'.");
            return writer;
        }

        public static Tuple<Func<IDecoder, object>, Action<IDecoder>> ResolveReader<T>(Schema readerSchema, Schema writerSchema)
        {
            var reader = ResolveReader(readerSchema, writerSchema, typeof(T));
            if (reader == null)
                throw new AvroException($"Unable to resolve reader for: '{readerSchema}' using writer: '{writerSchema}'.");
            return reader;
        }

        private static Action<IEncoder, object> ResolveWriter(Schema writerSchema, Type type)
        {
            var writer = default(Action<IEncoder, object>);
            switch (writerSchema)
            {
                case NullSchema _ when type.IsClass || Nullable.GetUnderlyingType(type) != null:
                    writer = (s, v) => s.WriteNull();
                    break;
                case BooleanSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(bool)):
                    writer = (s, v) => s.WriteBoolean((bool)v);
                    break;
                case IntSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(int)):
                    writer = (s, v) => s.WriteInt((int)v);
                    break;
                case LongSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(long)):
                    writer = (s, v) => s.WriteLong((long)v);
                    break;
                case FloatSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(float)):
                    writer = (s, v) => s.WriteFloat((float)v);
                    break;
                case DoubleSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(double)):
                    writer = (s, v) => s.WriteDouble((double)v);
                    break;
                case BytesSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(byte[])):
                    writer = (s, v) => s.WriteBytes((byte[])v);
                    break;
                case StringSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(string)):
                    writer = (s, v) => s.WriteString((string)v);
                    break;
                case UuidSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(Guid)):
                    writer = (s, v) => s.WriteUuid((Guid)v);
                    break;
                case DateSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(DateTime)):
                    writer = (s, v) => s.WriteDate((DateTime)v);
                    break;
                case TimeMillisSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(TimeSpan)):
                    writer = (s, v) => s.WriteTimeMS((TimeSpan)v);
                    break;
                case TimeMicrosSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(TimeSpan)):
                    writer = (s, v) => s.WriteTimeUS((TimeSpan)v);
                    break;
                case TimeNanosSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(TimeSpan)):
                    writer = (s, v) => s.WriteTimeNS((TimeSpan)v);
                    break;
                case TimestampMillisSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(DateTime)):
                    writer = (s, v) => s.WriteTimestampMS((DateTime)v);
                    break;
                case TimestampMicrosSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(DateTime)):
                    writer = (s, v) => s.WriteTimestampUS((DateTime)v);
                    break;
                case TimestampNanosSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(DateTime)):
                    writer = (s, v) => s.WriteTimestampNS((DateTime)v);
                    break;
                case DurationSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(ValueTuple<uint, uint, uint>)):
                    writer = (s, v) => s.WriteDuration((ValueTuple<uint, uint, uint>)v);
                    break;
                case DecimalSchema r when r.Type is BytesSchema && (type.Equals(typeof(object)) || type.Equals(typeof(decimal))):
                    writer = (s, v) => s.WriteDecimal((decimal)v, r.Scale);
                    break;
                case DecimalSchema r when r.Type is FixedSchema && (type.Equals(typeof(object)) || type.Equals(typeof(decimal))):
                    writer = (s, v) => s.WriteDecimal((decimal)v, r.Scale, (r.Type as FixedSchema).Size);
                    break;
                case ArraySchema r when type.Equals(typeof(object)) || typeof(IList<object>).IsAssignableFrom(type):
                    writer = (s, v) => s.WriteArray(v as IList<object>, ResolveWriter(r.Items, typeof(object)));
                    break;
                case MapSchema r when type.Equals(typeof(object)) || typeof(IDictionary<string, object>).IsAssignableFrom(type):
                    writer = (s, v) => s.WriteMap(v as IDictionary<string, object>, ResolveWriter(r.Values, typeof(object)));
                    break;
                case EnumSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(GenericEnum)):
                    writer = (s, v) => s.WriteInt((v as GenericEnum).Value);
                    break;
                case FixedSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(GenericFixed)):
                    writer = (s, v) => s.WriteFixed((v as GenericFixed).Value);
                    break;
                case RecordSchema r when type.Equals(typeof(object)) || type.Equals(typeof(GenericRecord)):
                    var fieldWriters = new Action<IEncoder, object>[r.Count];
                    for (int i = 0; i < fieldWriters.Length; i++)
                        fieldWriters[i] = ResolveWriter(r.ElementAt(i).Type, typeof(object));
                    writer = (s, v) =>
                    {
                        var record = v as GenericRecord;
                        for (int i = 0; i < fieldWriters.Length; i++)
                            fieldWriters[i].Invoke(s, record[i]);
                    };
                    break;
                case UnionSchema r when (Nullable.GetUnderlyingType(type) != null || type.IsClass) && r.Count == 2 && r.FirstOrDefault(n => n.GetType().Equals(typeof(NullSchema))) != null:
                    var nullIndex = 0;
                    if (!r[nullIndex].GetType().Equals(typeof(NullSchema)))
                        nullIndex = 1;
                    var notNullIndex = (nullIndex + 1L) % 2L;
                    var valueWriter = ResolveWriter(r[(int)notNullIndex], typeof(object));
                    writer = (s, v) =>
                    {
                        if (v == null)
                        {
                            s.WriteLong(nullIndex);
                        }
                        else
                        {
                            s.WriteLong(notNullIndex);
                            valueWriter.Invoke(s, v);
                        }
                    };
                    break;
                case UnionSchema r when type.Equals(typeof(object)) && r.Count > 0:
                    var map = new SortedList<Type, int>(r.Count, new TypeCompare());
                    var writers = new Action<IEncoder, object>[r.Count];
                    for (int i = 0; i < r.Count; i++)
                    {
                        if (r[i] is NullSchema)
                            map.Add(typeof(Nullable), i);
                        else
                            map.Add(GetTypeFromSchema(r[i]), i);
                        writers[i] = ResolveWriter(r[i], typeof(object));
                    }
                    writer = (s, v) =>
                    {
                        var index = 0L;
                        if (v == null)
                            index = map[typeof(Nullable)];
                        else
                            index = map[v.GetType()];
                        s.WriteLong(index);
                        writers[index].Invoke(s, v);
                    };
                    break;
            }
            return writer;
        }

        public class TypeCompare : IComparer<Type>
        {
            public int Compare(Type x, Type y)
            {
                return x.FullName.CompareTo(y.FullName);
            }
        }

        private static Tuple<Func<IDecoder, object>, Action<IDecoder>> ResolveReader(Schema readerSchema, Schema writerSchema, Type type)
        {
            var reader = default(Tuple<Func<IDecoder, object>, Action<IDecoder>>);
            switch (readerSchema)
            {
                case NullSchema r when writerSchema is NullSchema && (type.IsClass || Nullable.GetUnderlyingType(type) != null):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadNull(), s => s.SkipNull());
                    break;
                case BooleanSchema r when writerSchema is BooleanSchema && (type.Equals(typeof(object)) || type.Equals(typeof(bool))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadBoolean(), s => s.SkipBoolean());
                    break;
                case IntSchema r when writerSchema is IntSchema && (type.Equals(typeof(object)) || type.Equals(typeof(int))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadInt(), s => s.SkipInt());
                    break;
                case LongSchema r when writerSchema is LongSchema && (type.Equals(typeof(object)) || type.Equals(typeof(long))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadLong(), s => s.SkipLong());
                    break;
                case LongSchema r when writerSchema is IntSchema && (type.Equals(typeof(object)) || type.Equals(typeof(long))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => (long)s.ReadInt(), s => s.SkipInt());
                    break;
                case FloatSchema r when writerSchema is FloatSchema && (type.Equals(typeof(object)) || type.Equals(typeof(float))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadFloat(), s => s.SkipFloat());
                    break;
                case FloatSchema r when writerSchema is LongSchema && (type.Equals(typeof(object)) || type.Equals(typeof(float))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => (float)s.ReadLong(), s => s.SkipLong());
                    break;
                case FloatSchema r when writerSchema is IntSchema && (type.Equals(typeof(object)) || type.Equals(typeof(float))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => (float)s.ReadInt(), s => s.SkipInt());
                    break;
                case DoubleSchema r when writerSchema is DoubleSchema && (type.Equals(typeof(object)) || type.Equals(typeof(double))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadDouble(), s => s.SkipDouble());
                    break;
                case DoubleSchema r when writerSchema is FloatSchema && (type.Equals(typeof(object)) || type.Equals(typeof(double))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => (double)s.ReadFloat(), s => s.SkipFloat());
                    break;
                case DoubleSchema r when writerSchema is LongSchema && (type.Equals(typeof(object)) || type.Equals(typeof(double))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => (double)s.ReadLong(), s => s.SkipLong());
                    break;
                case DoubleSchema r when writerSchema is IntSchema && (type.Equals(typeof(object)) || type.Equals(typeof(double))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => (double)s.ReadInt(), s => s.SkipInt());
                    break;
                case BytesSchema r when writerSchema is BytesSchema && (type.Equals(typeof(object)) || type.Equals(typeof(byte[]))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadBytes(), s => s.SkipBytes());
                    break;
                case BytesSchema r when writerSchema is StringSchema && (type.Equals(typeof(object)) || type.Equals(typeof(byte[]))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadBytes(), s => s.SkipBytes());
                    break;
                case StringSchema r when writerSchema is StringSchema && (type.Equals(typeof(object)) || type.Equals(typeof(string))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadString(), s => s.SkipString());
                    break;
                case StringSchema r when writerSchema is BytesSchema && (type.Equals(typeof(object)) || type.Equals(typeof(string))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadString(), s => s.SkipString());
                    break;
                case UuidSchema r when writerSchema is UuidSchema && (type.Equals(typeof(object)) || type.Equals(typeof(Guid))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadUuid(), s => s.SkipUuid());
                    break;
                case DateSchema r when writerSchema is DateSchema && (type.Equals(typeof(object)) || type.Equals(typeof(DateTime))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadDate(), s => s.SkipDate());
                    break;
                case TimeMillisSchema r when writerSchema is TimeMillisSchema && (type.Equals(typeof(object)) || type.Equals(typeof(TimeSpan))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadTimeMS(), s => s.SkipTimeMS());
                    break;
                case TimeMicrosSchema r when writerSchema is TimeMicrosSchema && (type.Equals(typeof(object)) || type.Equals(typeof(TimeSpan))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadTimeUS(), s => s.SkipTimeUS());
                    break;
                case TimeMicrosSchema r when writerSchema is TimeMillisSchema && (type.Equals(typeof(object)) || type.Equals(typeof(TimeSpan))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadTimeMS(), s => s.SkipTimeMS());
                    break;
                case TimeNanosSchema r when writerSchema is TimeNanosSchema && (type.Equals(typeof(object)) || type.Equals(typeof(TimeSpan))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadTimeNS(), s => s.SkipTimeNS());
                    break;
                case TimeNanosSchema r when writerSchema is TimeMicrosSchema && (type.Equals(typeof(object)) || type.Equals(typeof(TimeSpan))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadTimeUS(), s => s.SkipTimeUS());
                    break;
                case TimeNanosSchema r when writerSchema is TimeMillisSchema && (type.Equals(typeof(object)) || type.Equals(typeof(TimeSpan))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadTimeMS(), s => s.SkipTimeMS());
                    break;
                case TimestampMillisSchema r when writerSchema is TimestampMillisSchema && (type.Equals(typeof(object)) || type.Equals(typeof(DateTime))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadTimestampMS(), s => s.SkipTimestampMS());
                    break;
                case TimestampMicrosSchema r when writerSchema is TimestampMicrosSchema && (type.Equals(typeof(object)) || type.Equals(typeof(DateTime))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadTimestampUS(), s => s.SkipTimestampUS());
                    break;
                case TimestampMicrosSchema r when writerSchema is TimestampMillisSchema && (type.Equals(typeof(object)) || type.Equals(typeof(DateTime))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadTimestampMS(), s => s.SkipTimestampMS());
                    break;
                case TimestampNanosSchema r when writerSchema is TimestampNanosSchema && (type.Equals(typeof(object)) || type.Equals(typeof(DateTime))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadTimestampNS(), s => s.SkipTimestampNS());
                    break;
                case TimestampNanosSchema r when writerSchema is TimestampMicrosSchema && (type.Equals(typeof(object)) || type.Equals(typeof(DateTime))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadTimestampUS(), s => s.SkipTimestampUS());
                    break;
                case TimestampNanosSchema r when writerSchema is TimestampMillisSchema && (type.Equals(typeof(object)) || type.Equals(typeof(DateTime))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadTimestampMS(), s => s.SkipTimestampMS());
                    break;
                case DurationSchema r when writerSchema is DurationSchema && (type.Equals(typeof(object)) || type.Equals(typeof(ValueTuple<uint, uint, uint>))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadDuration(), s => s.SkipDuration());
                    break;
                case DecimalSchema r when r.Equals(writerSchema) && (writerSchema as DecimalSchema).Type is BytesSchema && (type.Equals(typeof(object)) || type.Equals(typeof(decimal))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadDecimal(r.Scale), s => s.SkipDecimal());
                    break;
                case DecimalSchema r when r.Equals(writerSchema) && (writerSchema as DecimalSchema).Type is FixedSchema && (type.Equals(typeof(object)) || type.Equals(typeof(decimal))):
                    var decimalWriter = writerSchema as DecimalSchema;
                    var decimalLength = (decimalWriter.Type as FixedSchema).Size;
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadDecimal(r.Scale, decimalLength), s => s.SkipDecimal(decimalLength));
                    break;
                case ArraySchema r when writerSchema is ArraySchema && (type.Equals(typeof(object)) || typeof(IList<object>).IsAssignableFrom(type)):
                    var itemsReader = ResolveReader(r.Items, (writerSchema as ArraySchema).Items, typeof(object));
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadArray(itemsReader.Item1), s => s.SkipArray(itemsReader.Item2));
                    break;
                case MapSchema r when writerSchema is MapSchema && (type.Equals(typeof(object)) || typeof(IDictionary<string, object>).IsAssignableFrom(type)):
                    var valuesReader = ResolveReader(r.Values, (writerSchema as MapSchema).Values, typeof(object));
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadMap(valuesReader.Item1), s => s.SkipMap(valuesReader.Item2));
                    break;
                case EnumSchema r when writerSchema is EnumSchema && (type.Equals(typeof(object)) || type.Equals(typeof(GenericEnum))):
                    var writerSymbols = (writerSchema as EnumSchema).Symbols.ToArray();
                    var enumMap = new int[writerSymbols.Length];
                    for (int i = 0; i < writerSymbols.Length; i++)
                        enumMap[i] = r.Symbols.IndexOf(writerSymbols[i]);
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                        s =>
                        {
                            var value = s.ReadInt();
                            return new GenericEnum(r, enumMap[value]);
                        },
                        s => s.SkipInt()
                    );
                    break;
                case FixedSchema r when r.Equals(writerSchema) && (type.Equals(typeof(object)) || type.Equals(typeof(GenericFixed))):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                        s =>
                        {
                            var value = s.ReadFixed(r.Size);
                            return new GenericFixed(r, value);
                        },
                        s => s.SkipFixed(r.Size)
                    );
                    break;
                case RecordSchema r when r.Equals(writerSchema) && (type.Equals(typeof(object)) || type.Equals(typeof(GenericRecord))):
                    var writerFields = (writerSchema as RecordSchema).ToList();
                    var readerFields = r.ToList();
                    var recordMap = new int[writerFields.Count];
                    var fieldReaders = new Tuple<Func<IDecoder, object>, Action<IDecoder>>[writerFields.Count];

                    var missingFields = readerFields.Where(f => !writerFields.Any(w => w.Name == f.Name)).ToArray();
                    var missingDefaults = missingFields.Where(f => f.Default == null);
                    if (missingDefaults.Count() > 0)
                        throw new AvroException($"Unmapped field without default: '{string.Join(", ", missingDefaults.Select(f => f.Name))}'");

                    for (int i = 0; i < writerFields.Count; i++)
                    {
                        recordMap[i] = readerFields.IndexOf(writerFields[i]);
                        if (recordMap[i] == -1)
                            fieldReaders[i] = ResolveReader(writerFields[i].Type, writerFields[i].Type, typeof(object));
                        else
                            fieldReaders[i] = ResolveReader(readerFields[recordMap[i]].Type, writerFields[i].Type, typeof(object));
                    }

                    var recordStructure = new GenericRecord(r);

                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                        s =>
                        {
                            var record = new GenericRecord(recordStructure);
                            for (int i = 0; i < fieldReaders.Length; i++)
                                if (recordMap[i] == -1)
                                    fieldReaders[i].Item2.Invoke(s);
                                else
                                    record[recordMap[i]] = fieldReaders[i].Item1.Invoke(s);
                            return record;
                        },
                        s =>
                        {
                            foreach (var fieldReader in fieldReaders)
                                fieldReader.Item2.Invoke(s);
                        }
                    );
                    break;
                // Union: Reader and Writer are single Nullable Types
                case UnionSchema r when (Nullable.GetUnderlyingType(type) != null || type.IsClass) && r.Count == 2 && r.FirstOrDefault(n => n.GetType().Equals(typeof(NullSchema))) != null:
                    var nullableReadSchema = r.FirstOrDefault(n => !n.GetType().Equals(typeof(NullSchema)));
                    var nullableType = Nullable.GetUnderlyingType(type) ?? type;
                    switch (writerSchema)
                    {
                        // Writer is Null Type
                        case NullSchema _:
                            reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadNull(), s => s.SkipNull());
                            break;
                        // Writer is a Union with two types one being Null Type
                        case UnionSchema u when u.Count == 2 && u.FirstOrDefault(n => n.GetType().Equals(typeof(NullSchema))) != null:
                            var nullableWriterSchema = u.FirstOrDefault(n => !n.GetType().Equals(typeof(NullSchema)));
                            var nullIndex = 0L;
                            if (!u[(int)nullIndex].GetType().Equals(typeof(NullSchema)))
                                nullIndex = 1L;
                            var readerFunctions = ResolveReader(nullableReadSchema, nullableWriterSchema, typeof(object));
                            reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                                s =>
                                {
                                    var index = s.ReadLong();
                                    if (index == nullIndex)
                                        return s.ReadNull();
                                    return readerFunctions.Item1.Invoke(s);
                                },
                                s =>
                                {
                                    var index = s.ReadLong();
                                    if (index == nullIndex)
                                        s.SkipNull();
                                    else
                                        readerFunctions.Item2.Invoke(s);
                                }
                            );
                            break;
                        // Writer is an arbitrary Union
                        case UnionSchema u:
                            var unionReaders = new Tuple<Func<IDecoder, object>, Action<IDecoder>>[u.Count];
                            for (int i = 0; i < u.Count; i++)
                            {
                                var index = FindMatch(u[i], r.ToArray(), out var matchingSchema);
                                if (index == -1)
                                {
                                    var skipper = ResolveReader(u[i], u[i], typeof(object));
                                    unionReaders[i] = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                                        s => throw new IndexOutOfRangeException(),
                                        skipper.Item2
                                    );
                                }
                                else
                                {
                                    unionReaders[i] = ResolveReader(matchingSchema, u[i], typeof(object));
                                }
                            }
                            reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                                s =>
                                {
                                    var index = s.ReadLong();
                                    return unionReaders[(int)index].Item1.Invoke(s);
                                },
                                s =>
                                {
                                    var index = s.ReadLong();
                                    unionReaders[(int)index].Item2.Invoke(s);
                                }
                            );
                            break;
                        // Writer is not a Union nor a Null Type
                        default:
                            reader = ResolveReader(nullableReadSchema, writerSchema, typeof(object));
                            break;
                    }
                    break;
                // Union: Reader is a Union but writer is not
                case UnionSchema r when type.Equals(typeof(object)) && !(writerSchema is UnionSchema):
                    var nonUnionToUnionIndex = FindMatch(writerSchema, r.ToArray(), out var nonUnionToUnionMatch);
                    if (nonUnionToUnionIndex >= 0)
                        reader = ResolveReader(nonUnionToUnionMatch, writerSchema, typeof(object));
                    break;
                // Union: Reader is a Union and Writer is a Union
                case UnionSchema r when type.Equals(typeof(object)) && writerSchema is UnionSchema && (writerSchema as UnionSchema).Count > 0:
                    var unionToUnionWriterSchemas = (writerSchema as UnionSchema).ToArray();
                    var unionToUnuionReaders = new Tuple<Func<IDecoder, object>, Action<IDecoder>>[unionToUnionWriterSchemas.Length];

                    for (int i = 0; i < unionToUnionWriterSchemas.Length; i++)
                    {
                        var index = FindMatch(unionToUnionWriterSchemas[i], r.ToArray(), out var matchingSchema);
                        if (index == -1)
                        {
                            var skipper = ResolveReader(unionToUnionWriterSchemas[i], unionToUnionWriterSchemas[i], typeof(object));
                            unionToUnuionReaders[i] = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                                s => throw new IndexOutOfRangeException(),
                                skipper.Item2
                            );
                        }
                        else
                        {
                            unionToUnuionReaders[i] = ResolveReader(matchingSchema, unionToUnionWriterSchemas[i], typeof(object));
                        }
                    }
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                        s =>
                        {
                            var index = s.ReadLong();
                            return unionToUnuionReaders[(int)index].Item1.Invoke(s);
                        },
                        s =>
                        {
                            var index = s.ReadLong();
                            unionToUnuionReaders[(int)index].Item2.Invoke(s);
                        }
                    );
                    break;
                // Union Type to Single Type
                case Schema r when writerSchema is UnionSchema && (writerSchema as UnionSchema).Count > 0:
                    var unionToNonUnionWriterSchemas = (writerSchema as UnionSchema).ToArray();
                    var unionToUNonnuionReaders = new Tuple<Func<IDecoder, object>, Action<IDecoder>>[unionToNonUnionWriterSchemas.Length];

                    for (int i = 0; i < unionToNonUnionWriterSchemas.Length; i++)
                    {
                        var unionToUNonnuionReader = ResolveReader(r, unionToNonUnionWriterSchemas[i], typeof(object));
                        if (unionToUNonnuionReader == null)
                        {
                            unionToUNonnuionReader = ResolveReader(unionToNonUnionWriterSchemas[i], unionToNonUnionWriterSchemas[i], typeof(object));
                            unionToUNonnuionReader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                                s => throw new IndexOutOfRangeException(),
                                unionToUNonnuionReader.Item2
                            );
                        }

                        unionToUNonnuionReaders[i] = unionToUNonnuionReader;
                    }
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                        s =>
                        {
                            var index = s.ReadLong();
                            return unionToUNonnuionReaders[(int)index].Item1.Invoke(s);
                        },
                        s =>
                        {
                            var index = s.ReadLong();
                            unionToUNonnuionReaders[(int)index].Item2.Invoke(s);
                        }
                    );
                    break;
            }
            return reader;
        }

        public static Func<object> GetDefaultInitialization(Schema schema, JToken value)
        {
            var defaultInit = default(Func<object>);
            switch (schema)
            {
                case NullSchema _:
                    defaultInit = () => null;
                    break;
                case BooleanSchema _:
                    var boolValue = bool.Parse(value.ToString().ToLower());
                    defaultInit = () => boolValue;
                    break;
                case IntSchema _:
                    var intValue = int.Parse(value.ToString());
                    defaultInit = () => intValue;
                    break;
                case LongSchema _:
                    var longValue = long.Parse(value.ToString());
                    defaultInit = () => long.Parse(value.ToString());
                    break;
                case FloatSchema _:
                    var floatValue = float.Parse(value.ToString());
                    defaultInit = () => floatValue;
                    break;
                case DoubleSchema _:
                    var doubleValue = double.Parse(value.ToString());
                    defaultInit = () => doubleValue;
                    break;
                case BytesSchema _:
                    var byteCodes = value.ToString().Split("\\u", StringSplitOptions.RemoveEmptyEntries);
                    var bytesValue = new byte[byteCodes.Length];
                    for (int i = 0; i < byteCodes.Length; i++)
                        bytesValue[i] = byte.Parse(byteCodes[i], System.Globalization.NumberStyles.HexNumber);
                    defaultInit = () => bytesValue.Clone();
                    break;
                case StringSchema _:
                    var stringValue = value.ToString().Trim('"');
                    defaultInit = () => string.Copy(stringValue);
                    break;
                case ArraySchema a:
                    var arrayItems = value as JArray;
                    var arrayItemsCount = arrayItems.Count;
                    var arrayItemInitializers = new Func<object>[arrayItemsCount];
                    for (int i = 0; i < arrayItemsCount; i++)
                        arrayItemInitializers[i] = GetDefaultInitialization(a.Items, arrayItems[i]);
                    defaultInit = () =>
                    {
                        var array = new List<object>();
                        foreach (var arrayItemInitializer in arrayItemInitializers)
                            array.Add(arrayItemInitializer.Invoke());
                        return array;
                    };
                    break;
                case MapSchema m:
                    var mapItems = (value as JObject).Properties().ToArray();
                    var mapItemsCount = mapItems.Length;
                    var mapItemInitializers = new ValueTuple<string, Func<object>>[mapItemsCount];
                    for (int i = 0; i < mapItemsCount; i++)
                        mapItemInitializers[i] = new ValueTuple<string, Func<object>>(mapItems[i].Name, GetDefaultInitialization(m.Values, mapItems[i].Value));
                    defaultInit = () =>
                    {
                        var map = new Dictionary<string, object>();
                        foreach (var mapItemInitializer in mapItemInitializers)
                            map.Add(mapItemInitializer.Item1, mapItemInitializer.Item2.Invoke());
                        return map;
                    };
                    break;
                case FixedSchema f:
                    var fixedCodes = value.ToString().Split("\\u", StringSplitOptions.RemoveEmptyEntries);
                    var fixedValue = new byte[fixedCodes.Length];
                    for (int i = 0; i < fixedCodes.Length; i++)
                        fixedValue[i] = byte.Parse(fixedCodes[i], System.Globalization.NumberStyles.HexNumber);
                    defaultInit = () => new GenericFixed(f, fixedValue.Clone() as byte[]);
                    break;
                case EnumSchema e:
                    defaultInit = () => new GenericEnum(e, value.ToString().Trim('"'));
                    break;
                case RecordSchema r:
                    var recordFields = r.ToList();
                    var defaultFields =
                        from f in recordFields
                        join p in (value as JObject).Properties() on f.Name equals p.Name
                        select new
                        {
                            Field = f,
                            p.Value
                        }
                    ;

                    var defaultAssignment =
                        from d in defaultFields
                        select new
                        {
                            FieldIndex = recordFields.IndexOf(d.Field),
                            Initializer = GetDefaultInitialization(d.Field.Type, d.Value)
                        }
                    ;
                    defaultInit = () =>
                    {
                        var record = new GenericRecord(r);
                        foreach (var fieldInitializer in defaultAssignment)
                            record[fieldInitializer.FieldIndex] = fieldInitializer.Initializer.Invoke();
                        return record;
                    };
                    break;
                case UnionSchema u:
                    defaultInit = GetDefaultInitialization(u[0], value);
                    break;
                case UuidSchema _:
                    defaultInit = () => new Guid(value.ToString().Trim('"'));
                    break;
                case LogicalSchema l:
                    defaultInit = GetDefaultInitialization(l.Type, value);
                    break;
            }
            return defaultInit;
        }

        public static Type GetTypeFromSchema(Schema schema)
        {
            switch (schema)
            {
                case NullSchema _:
                    return typeof(object);
                case BooleanSchema _:
                    return typeof(bool);
                case IntSchema _:
                    return typeof(int);
                case LongSchema _:
                    return typeof(long);
                case FloatSchema _:
                    return typeof(float);
                case DoubleSchema _:
                    return typeof(double);
                case BytesSchema _:
                    return typeof(byte[]);
                case StringSchema _:
                    return typeof(string);
                case ArraySchema _:
                    return typeof(IList<object>);
                case MapSchema _:
                    return typeof(IDictionary<string, object>);
                case DecimalSchema _:
                    return typeof(decimal);
                case DateSchema _:
                    return typeof(DateTime);
                case TimestampMillisSchema _:
                    return typeof(DateTime);
                case TimestampMicrosSchema _:
                    return typeof(DateTime);
                case TimestampNanosSchema _:
                    return typeof(DateTime);
                case TimeMillisSchema _:
                    return typeof(TimeSpan);
                case TimeMicrosSchema _:
                    return typeof(TimeSpan);
                case TimeNanosSchema _:
                    return typeof(TimeSpan);
                case DurationSchema _:
                    return typeof(ValueTuple<uint, uint, uint>);
                case UuidSchema _:
                    return typeof(Guid);
                case EnumSchema _:
                    return typeof(GenericEnum);
                case FixedSchema _:
                    return typeof(GenericFixed);
                case RecordSchema _:
                    return typeof(GenericRecord);
                case UnionSchema r:
                    if (r.Count == 2 && r.Any(n => n.GetType().Equals(typeof(NullSchema))))
                    {
                        var nullableSchema = r.First(s => !s.GetType().Equals(typeof(NullSchema)));
                        switch (nullableSchema)
                        {
                            case IntSchema _:
                            case LongSchema _:
                            case FloatSchema _:
                            case DoubleSchema _:
                            case DecimalSchema _:
                            case DateSchema _:
                            case TimestampMillisSchema _:
                            case TimestampMicrosSchema _:
                            case TimestampNanosSchema _:
                            case TimeMillisSchema _:
                            case TimeMicrosSchema _:
                            case TimeNanosSchema _:
                            case UuidSchema _:
                                return typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(nullableSchema));
                            default:
                                return GetTypeFromSchema(nullableSchema);
                        }
                    }
                    return typeof(object);
                case LogicalSchema r:
                    return GetTypeFromSchema(r.Type);
                default:
                    throw new ArgumentException($"Unsupported schema: '{schema.GetType().Name}'");
            }
        }

        private static int FindMatch(Schema schema, Schema[] schemas, out Schema matchingSchema)
        {
            switch (schema)
            {
                case IntSchema s:
                    matchingSchema =
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(IntSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(LongSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(FloatSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(DoubleSchema)))
                    ;
                    break;
                case LongSchema s:
                    matchingSchema =
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(LongSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(FloatSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(DoubleSchema)))
                    ;
                    break;
                case FloatSchema s:
                    matchingSchema =
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(DoubleSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(FloatSchema)))
                    ;
                    break;
                case StringSchema s:
                    matchingSchema =
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(StringSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(BytesSchema)))
                    ;
                    break;
                case BytesSchema s:
                    matchingSchema =
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(BytesSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(StringSchema)))
                    ;
                    break;
                case TimeMillisSchema s:
                    matchingSchema =
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimeMillisSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimeMicrosSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimeNanosSchema)))
                    ;
                    break;
                case TimeMicrosSchema s:
                    matchingSchema =
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimeMicrosSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimeNanosSchema)))
                    ;
                    break;
                case TimestampMillisSchema s:
                    matchingSchema =
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimestampMillisSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimestampMicrosSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimestampNanosSchema)))
                    ;
                    break;
                case TimestampMicrosSchema s:
                    matchingSchema =
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimestampMicrosSchema))) ??
                        schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimestampNanosSchema)))
                    ;
                    break;
                default:
                    matchingSchema = schemas.FirstOrDefault(r => r.Equals(schema));
                    break;
            }

            if (matchingSchema != null)
                return Array.IndexOf(schemas, matchingSchema);
            return -1;
        }
    }
}
