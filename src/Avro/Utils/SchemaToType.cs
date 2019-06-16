using Avro.Schemas;
using System;
using System.Linq;
using System.Reflection;

namespace Avro.Utils
{
    public static class SchemaToType
    {
        public static Type Get(Schema schema, Assembly assembly)
        {
            switch (schema)
            {
                case NullSchema r:
                    return typeof(object);
                case BooleanSchema r:
                    return typeof(bool);
                case IntSchema r:
                    return typeof(int);
                case LongSchema r:
                    return typeof(long);
                case FloatSchema r:
                    return typeof(float);
                case DoubleSchema r:
                    return typeof(double);
                case BytesSchema r:
                    return typeof(byte[]);
                case StringSchema r:
                    return typeof(string);
                case DateSchema r:
                    return typeof(DateTime);
                case TimestampMillisSchema r:
                    return typeof(DateTime);
                case TimestampMicrosSchema r:
                    return typeof(DateTime);
                case TimestampNanosSchema r:
                    return typeof(DateTime);
                case TimeMillisSchema r:
                    return typeof(TimeSpan);
                case TimeMicrosSchema r:
                    return typeof(TimeSpan);
                case TimeNanosSchema r:
                    return typeof(TimeSpan);
                case DurationSchema r:
                    return typeof(Tuple<int, int, int>);
                case UuidSchema r:
                    return typeof(Guid);
                case EnumSchema r:
                    return assembly.GetType(r.FullName);
                case FixedSchema r:
                    return assembly.GetType(r.FullName);
                case RecordSchema r:
                    return assembly.GetType(r.FullName);
                case UnionSchema r:
                    if (r.Count == 2 && r.Any(n => n.GetType().Equals(typeof(NullSchema))))
                    {
                        var nullableSchema = r.First(s => !s.GetType().Equals(typeof(NullSchema)));
                        switch (nullableSchema)
                        {
                            case IntSchema a:
                            case LongSchema b:
                            case FloatSchema c:
                            case DoubleSchema d:
                            case DateSchema e:
                            case TimestampMillisSchema f:
                            case TimestampMicrosSchema g:
                            case TimestampNanosSchema h:
                            case TimeMillisSchema j:
                            case TimeMicrosSchema k:
                            case TimeNanosSchema l:
                            case UuidSchema m:
                            case EnumSchema n:
                                return typeof(Nullable<>).MakeGenericType(Get(nullableSchema, assembly));
                            default:
                                return Get(nullableSchema, assembly);
                        }
                    }
                    return typeof(object);
                default:
                    throw new ArgumentException($"Unsupported schema: '{schema.GetType().Name}'");
            }
        }
    }
}
