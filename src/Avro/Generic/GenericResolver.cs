using Avro.IO;
using Avro.Schemas;
using Avro.Types;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Generic
{
    public static partial class GenericResolver
    {
        public static Func<object> GetDefaultInitialization(AvroSchema schema, JToken value)
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
                    defaultInit = () => new GenericAvroFixed(f, fixedValue.Clone() as byte[]);
                    break;
                case EnumSchema e:
                    defaultInit = () => new GenericAvroEnum(e, value.ToString().Trim('"'));
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
                        var record = new GenericAvroRecord(r);
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

        public static Type GetTypeFromSchema(AvroSchema schema)
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
                    return typeof(AvroDuration);
                case UuidSchema _:
                    return typeof(Guid);
                case EnumSchema _:
                    return typeof(GenericAvroEnum);
                case FixedSchema _:
                    return typeof(GenericAvroFixed);
                case RecordSchema _:
                    return typeof(GenericAvroRecord);
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

        private static int FindMatch(AvroSchema schema, AvroSchema[] schemas, out AvroSchema matchingSchema)
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

        public class TypeCompare : IComparer<Type>
        {
            public int Compare(Type x, Type y)
            {
                return x.FullName.CompareTo(y.FullName);
            }
        }
    }
}
