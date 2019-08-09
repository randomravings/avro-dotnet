using Avro.Schemas;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Avro.Specific
{
    public static partial class SpecificResolver
    {
        private static Expression GetValueExpression(ParameterExpression valueParameter, Stack<PropertyInfo> propertyChain)
        {
            var valueExpression = valueParameter as Expression;
            foreach (var property in propertyChain.Reverse())
                valueExpression =
                    Expression.MakeMemberAccess(
                        valueExpression,
                        property
                    );
            return valueExpression;
        }

        private static Expression CastOrExpression(Expression expression, Type type)
        {
            if (type == null)
                return expression;
            return
                Expression.Convert(
                    expression,
                    type
                );
        }

        public static Type GetTypeFromSchema(Schema schema, Assembly assembly)
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
                case ArraySchema r:
                    return typeof(IList<>).MakeGenericType(GetTypeFromSchema(r.Items, assembly));
                case MapSchema r:
                    return typeof(IDictionary<,>).MakeGenericType(typeof(string), GetTypeFromSchema(r.Values, assembly));
                case DecimalSchema r:
                    return typeof(decimal);
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
                    return typeof(ValueTuple<uint, uint, uint>);
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
                            case DecimalSchema e:
                            case DateSchema f:
                            case TimestampMillisSchema g:
                            case TimestampMicrosSchema h:
                            case TimestampNanosSchema i:
                            case TimeMillisSchema j:
                            case TimeMicrosSchema k:
                            case TimeNanosSchema l:
                            case UuidSchema m:
                            case EnumSchema n:
                                return typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(nullableSchema, assembly));
                            default:
                                return GetTypeFromSchema(nullableSchema, assembly);
                        }
                    }
                    return typeof(object);
                case LogicalSchema r:
                    return GetTypeFromSchema(r.Type, assembly);
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
