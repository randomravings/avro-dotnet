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
        public static Action<IEncoder, object> ResolveWriter<T>(AvroSchema writerSchema)
        {
            var writer = ResolveWriter(writerSchema, typeof(T));
            if (writer == null)
                throw new AvroException($"Unable to resolve writer for: '{writerSchema}'.");
            return writer;
        }

        private static Action<IEncoder, object> ResolveWriter(AvroSchema writerSchema, Type type)
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
                case DurationSchema _ when type.Equals(typeof(object)) || type.Equals(typeof(AvroDuration)):
                    writer = (s, v) => s.WriteDuration((AvroDuration)v);
                    break;
                case DecimalSchema r when r.Type is BytesSchema && (type.Equals(typeof(object)) || type.Equals(typeof(decimal))):
                    writer = (s, v) => s.WriteDecimal((decimal)v, r.Scale);
                    break;
                case DecimalSchema r when r.Type is FixedSchema && (type.Equals(typeof(object)) || type.Equals(typeof(decimal))):
                    writer = (s, v) => s.WriteDecimal((decimal)v, r.Scale, (r.Type as FixedSchema).Size);
                    break;
                case ArraySchema r when type.Equals(typeof(object)) || (type.Equals(typeof(IList<object>))):
                    var itemsWriter = ResolveWriter(r.Items, typeof(object));
                    writer = (s, v) => s.WriteArray(v as IList<object>, itemsWriter);
                    break;
                case MapSchema r when type.Equals(typeof(object)) || (type.IsGenericType && type.Equals(typeof(IDictionary<string, object>))):
                    var valuesWriter = ResolveWriter(r.Values, typeof(object));
                    writer = (s, v) => s.WriteMap(v as IDictionary<string, object>, valuesWriter);
                    break;
                case EnumSchema _ when type.Equals(typeof(object)) || typeof(GenericAvroEnum).IsAssignableFrom(type):
                    writer = (s, v) => s.WriteInt((v as GenericAvroEnum));
                    break;
                case FixedSchema _ when type.Equals(typeof(object)) || typeof(GenericAvroFixed).IsAssignableFrom(type):
                    writer = (s, v) => s.WriteFixed((v as GenericAvroFixed));
                    break;
                case ErrorSchema r when type.Equals(typeof(object)) || type.Equals(typeof(GenericAvroError)):
                    var errorRecordWriter = ResolveWriter(writerSchema as RecordSchema, typeof(GenericAvroRecord));
                    writer = (s, v) =>
                    {
                        errorRecordWriter.Invoke(s, (v as GenericAvroError));
                    };
                    break;
                case RecordSchema r when type.Equals(typeof(object)) || typeof(GenericAvroRecord).IsAssignableFrom(type):
                    var fieldWriters = new Action<IEncoder, object>[r.Count];
                    for (int i = 0; i < fieldWriters.Length; i++)
                        fieldWriters[i] = ResolveWriter(r.ElementAt(i).Type, typeof(object));
                    writer = (s, v) =>
                    {
                        var record = v as GenericAvroRecord;
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
    }
}
