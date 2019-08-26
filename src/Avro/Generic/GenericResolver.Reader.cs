using Avro.IO;
using Avro.Schemas;
using Avro.Types;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Generic
{
    public static partial class GenericResolver
    {
        public static Tuple<Func<IDecoder, object>, Action<IDecoder>> ResolveReader<T>(AvroSchema readerSchema, AvroSchema writerSchema)
        {
            var reader = ResolveReader(readerSchema, writerSchema, typeof(T));
            if (reader == null)
                throw new AvroException($"Unable to resolve reader for: '{readerSchema}' using writer: '{writerSchema}'.");
            return reader;
        }

        private static Tuple<Func<IDecoder, object>, Action<IDecoder>> ResolveReader(AvroSchema readerSchema, AvroSchema writerSchema, Type type)
        {
            var reader = default(Tuple<Func<IDecoder, object>, Action<IDecoder>>);
            switch (readerSchema)
            {
                case NullSchema r when writerSchema is NullSchema && (type.IsClass || Nullable.GetUnderlyingType(type) != null):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => { s.ReadNull(); return null; }, s => s.SkipNull());
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
                case DurationSchema r when writerSchema is DurationSchema && (type.Equals(typeof(object)) || type.Equals(typeof(AvroDuration))):
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
                case ArraySchema r when writerSchema is ArraySchema && (type.Equals(typeof(object)) || type.Equals(typeof(IList<object>))):
                    var itemsReader = ResolveReader(r.Items, (writerSchema as ArraySchema).Items, typeof(object));
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadArray(itemsReader.Item1), s => s.SkipArray(itemsReader.Item2));
                    break;
                case MapSchema r when writerSchema is MapSchema && (type.Equals(typeof(object)) || (type.IsGenericType && type.GetGenericTypeDefinition().Equals(typeof(IDictionary<,>)) && type.GetGenericArguments().First().Equals(typeof(string)))):
                    var mapType = type.GetGenericArguments().LastOrDefault() ?? typeof(object);
                    var valuesReader = ResolveReader(r.Values, (writerSchema as MapSchema).Values, mapType);
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => s.ReadMap(valuesReader.Item1), s => s.SkipMap(valuesReader.Item2));
                    break;
                case EnumSchema r when writerSchema is EnumSchema && (type.Equals(typeof(object)) || typeof(GenericAvroEnum).IsAssignableFrom(type)):
                    var writerSymbols = (writerSchema as EnumSchema).Symbols.ToArray();
                    var enumMap = new int[writerSymbols.Length];
                    for (int i = 0; i < writerSymbols.Length; i++)
                        enumMap[i] = r.Symbols.IndexOf(writerSymbols[i]);
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                        s =>
                        {
                            var value = s.ReadInt();
                            var enumValue = new GenericAvroEnum(r, enumMap[value]);
                            return enumValue;
                        },
                        s => s.SkipInt()
                    );
                    break;
                case FixedSchema r when r.Equals(writerSchema) && (type.Equals(typeof(object)) || typeof(GenericAvroFixed).IsAssignableFrom(type)):
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                        s =>
                        {
                            var value = s.ReadFixed(r.Size);
                            var fixedValue = new GenericAvroFixed(r, value);
                            return fixedValue;
                        },
                        s => s.SkipFixed(r.Size)
                    );
                    break;
                case ErrorSchema r when r.Equals(writerSchema) && (type.Equals(typeof(object)) || type.Equals(typeof(GenericAvroError))):
                    var errorRecordReader = ResolveReader(readerSchema as RecordSchema, writerSchema as RecordSchema, typeof(GenericAvroRecord));
                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                        s =>
                        {
                            var record = errorRecordReader.Item1.Invoke(s) as GenericAvroRecord;
                            return new GenericAvroError(new AvroException(r.FullName), record);
                        },
                        s => errorRecordReader.Item2.Invoke(s)
                    );
                    break;
                case RecordSchema r when r.Equals(writerSchema) && (type.Equals(typeof(object)) || typeof(GenericAvroRecord).IsAssignableFrom(type)):
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

                    var recordStructure = new GenericAvroRecord(r);

                    reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(
                        s =>
                        {
                            var record = new GenericAvroRecord(recordStructure);
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
                            reader = new Tuple<Func<IDecoder, object>, Action<IDecoder>>(s => { s.ReadNull(); return null; }, s => s.SkipNull());
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
                                    {
                                        s.ReadNull();
                                        return null;
                                    }
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
                case AvroSchema r when writerSchema is UnionSchema && (writerSchema as UnionSchema).Count > 0:
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
    }
}
