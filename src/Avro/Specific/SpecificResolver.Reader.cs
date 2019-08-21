using Avro.IO;
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
        public static Tuple<Func<IDecoder, T>, Action<IDecoder>> ResolveReader<T>(Schema readerSchema, Schema writerSchema)
        {
            var type = typeof(T);
            var readFunction = typeof(Func<,>).MakeGenericType(typeof(IDecoder), type);
            var skipAction = typeof(Action<>).MakeGenericType(typeof(IDecoder));
            var streamParameter = Expression.Parameter(typeof(IDecoder), "s");

            var expressions = ResolveReader(type.Assembly, type, readerSchema, writerSchema, streamParameter);
            if (expressions == null)
                throw new AvroException($"Unable to resolve reader: '{readerSchema}' using writer: '{writerSchema}' for type: '{type}'");

            var readLambdaExpression =
                Expression.Lambda(
                    readFunction,
                    expressions.Item1,
                    streamParameter
                )
                .Compile() as Func<IDecoder, T>;

            var skipLambdaExpression =
                Expression.Lambda(
                    skipAction,
                    expressions.Item2,
                    streamParameter
                )
                .Compile() as Action<IDecoder>;

            return new Tuple<Func<IDecoder, T>, Action<IDecoder>>(
                readLambdaExpression,
                skipLambdaExpression
            );
        }

        private static Tuple<Expression, Expression> ResolveReader(Assembly origin, Type type, Schema readerSchema, Schema writerSchema, ParameterExpression streamParameter)
        {
            var expressions = default(Tuple<Expression, Expression>);

            switch (readerSchema)
            {
                case NullSchema r when writerSchema is NullSchema && (type.IsInterface || type.IsClass || (type.IsValueType && Nullable.GetUnderlyingType(type) != null)):
                    expressions = ResolveNull(streamParameter, type);
                    break;
                case BooleanSchema r when writerSchema is BooleanSchema && type.Equals(typeof(bool)):
                    expressions = ResolveBoolean(streamParameter);
                    break;
                case IntSchema r when writerSchema is IntSchema && type.Equals(typeof(int)):
                    expressions = ResolveInt(streamParameter);
                    break;
                case LongSchema r when writerSchema is LongSchema && type.Equals(typeof(long)):
                    expressions = ResolveLong(streamParameter);
                    break;
                case LongSchema r when writerSchema is IntSchema && type.Equals(typeof(long)):
                    expressions = ResolveLongFromInt(streamParameter);
                    break;
                case FloatSchema r when writerSchema is FloatSchema && type.Equals(typeof(float)):
                    expressions = ResolveFloat(streamParameter);
                    break;
                case FloatSchema r when writerSchema is LongSchema && type.Equals(typeof(float)):
                    expressions = ResolveFloatFromLong(streamParameter);
                    break;
                case FloatSchema r when writerSchema is IntSchema && type.Equals(typeof(float)):
                    expressions = ResolveFloatFromInt(streamParameter);
                    break;
                case DoubleSchema r when writerSchema is DoubleSchema && type.Equals(typeof(double)):
                    expressions = ResolveDouble(streamParameter);
                    break;
                case DoubleSchema r when writerSchema is FloatSchema && type.Equals(typeof(double)):
                    expressions = ResolveDoubleFromFloat(streamParameter);
                    break;
                case DoubleSchema r when writerSchema is LongSchema && type.Equals(typeof(double)):
                    expressions = ResolveDoubleFromLong(streamParameter);
                    break;
                case DoubleSchema r when writerSchema is IntSchema && type.Equals(typeof(double)):
                    expressions = ResolveDoubleFromInt(streamParameter);
                    break;
                case BytesSchema r when writerSchema is BytesSchema && type.Equals(typeof(byte[])):
                    expressions = ResolveBytes(streamParameter);
                    break;
                case BytesSchema r when writerSchema is StringSchema && type.Equals(typeof(byte[])):
                    expressions = ResolveBytes(streamParameter);
                    break;
                case StringSchema r when writerSchema is StringSchema && type.Equals(typeof(string)):
                    expressions = ResolveString(streamParameter);
                    break;
                case StringSchema r when writerSchema is BytesSchema && type.Equals(typeof(string)):
                    expressions = ResolveString(streamParameter);
                    break;
                case UuidSchema r when writerSchema is UuidSchema && type.Equals(typeof(Guid)):
                    expressions = ResolveUuid(streamParameter);
                    break;
                case DateSchema r when writerSchema is DateSchema && type.Equals(typeof(DateTime)):
                    expressions = ResolveDate(streamParameter);
                    break;
                case TimeMillisSchema r when writerSchema is TimeMillisSchema && type.Equals(typeof(TimeSpan)):
                    expressions = ResolveTimeMs(streamParameter);
                    break;
                case TimeMicrosSchema r when writerSchema is TimeMicrosSchema && type.Equals(typeof(TimeSpan)):
                    expressions = ResolveTimeUs(streamParameter);
                    break;
                case TimeMicrosSchema r when writerSchema is TimeMillisSchema && type.Equals(typeof(TimeSpan)):
                    expressions = ResolveTimeUsFromMs(streamParameter);
                    break;
                case TimeNanosSchema r when writerSchema is TimeNanosSchema && type.Equals(typeof(TimeSpan)):
                    expressions = ResolveTimeNs(streamParameter);
                    break;
                case TimeNanosSchema r when writerSchema is TimeMicrosSchema && type.Equals(typeof(TimeSpan)):
                    expressions = ResolveTimeNsFromUs(streamParameter);
                    break;
                case TimeNanosSchema r when writerSchema is TimeMillisSchema && type.Equals(typeof(TimeSpan)):
                    expressions = ResolveTimeNsFromMs(streamParameter);
                    break;
                case TimestampMillisSchema r when writerSchema is TimestampMillisSchema && type.Equals(typeof(DateTime)):
                    expressions = ResolveTimestampMs(streamParameter);
                    break;
                case TimestampMicrosSchema r when writerSchema is TimestampMicrosSchema && type.Equals(typeof(DateTime)):
                    expressions = ResolveTimestampUs(streamParameter);
                    break;
                case TimestampMicrosSchema r when writerSchema is TimestampMillisSchema && type.Equals(typeof(DateTime)):
                    expressions = ResolveTimestampUsFromMs(streamParameter);
                    break;
                case TimestampNanosSchema r when writerSchema is TimestampNanosSchema && type.Equals(typeof(DateTime)):
                    expressions = ResolveTimestampNs(streamParameter);
                    break;
                case TimestampNanosSchema r when writerSchema is TimestampMicrosSchema && type.Equals(typeof(DateTime)):
                    expressions = ResolveTimestampNsFromUs(streamParameter);
                    break;
                case TimestampNanosSchema r when writerSchema is TimestampMillisSchema && type.Equals(typeof(DateTime)):
                    expressions = ResolveTimestampNsFromMs(streamParameter);
                    break;
                case DurationSchema r when writerSchema is DurationSchema && type.Equals(typeof(ValueTuple<uint, uint, uint>)):
                    expressions = ResolveDuration(streamParameter);
                    break;
                case DecimalSchema r when r.Equals(writerSchema) && (writerSchema as DecimalSchema).Type is BytesSchema:
                    expressions = ResolveDecimal(streamParameter, r.Scale);
                    break;
                case DecimalSchema r when r.Equals(writerSchema) && (writerSchema as DecimalSchema).Type is FixedSchema:
                    var decimalWriter = writerSchema as DecimalSchema;
                    var decimalLength = (decimalWriter.Type as FixedSchema).Size;
                    expressions = ResolveDecimalFixed(streamParameter, r.Scale, decimalLength);
                    break;
                case ArraySchema r when writerSchema is ArraySchema && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IList<>):
                    expressions = ResolveArray(streamParameter, type.GenericTypeArguments.Last(), origin, r.Items, (writerSchema as ArraySchema).Items);
                    break;
                case MapSchema r when writerSchema is MapSchema && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IDictionary<,>):
                    expressions = ResolveMap(streamParameter, type.GenericTypeArguments.Last(), origin, r.Values, (writerSchema as MapSchema).Values);
                    break;

                case EnumSchema r when writerSchema is EnumSchema && r.Equals(writerSchema) && type.IsEnum && Enum.GetNames(type).Intersect(r.Symbols).Count() == r.Symbols.Count:
                    expressions = ResolveEnum(streamParameter, type, r.Symbols, (writerSchema as EnumSchema).Symbols);
                    break;
                case FixedSchema r when writerSchema is FixedSchema && r.Equals(writerSchema) && typeof(ISpecificFixed).IsAssignableFrom(type):
                    expressions = ResolveFixed(streamParameter, type, r.Size);
                    break;
                case RecordSchema r when writerSchema is RecordSchema && r.Equals(writerSchema) && typeof(ISpecificRecord).IsAssignableFrom(type):
                    expressions = ResolveRecord(streamParameter, type, origin, r, (writerSchema as RecordSchema));
                    break;

                // Union: Reader and Writer are single Nullable Types
                case UnionSchema r when (Nullable.GetUnderlyingType(type) != null || type.IsInterface || type.IsClass) && r.Count == 2 && r.FirstOrDefault(n => n.GetType().Equals(typeof(NullSchema))) != null:
                    var nullableReadSchema = r.FirstOrDefault(n => !n.GetType().Equals(typeof(NullSchema)));
                    var nullableType = Nullable.GetUnderlyingType(type) ?? type;
                    switch (writerSchema)
                    {
                        // Writer is Null Type
                        case NullSchema s:
                            expressions = ResolveNull(streamParameter, type);
                            break;
                        // Writer is a Union with two types one being Null Type
                        case UnionSchema s when s.Count == 2 && s.FirstOrDefault(n => n.GetType().Equals(typeof(NullSchema))) != null:
                            var nullableWriterSchema = s.FirstOrDefault(n => !n.GetType().Equals(typeof(NullSchema)));
                            var nullIndex = 0L;
                            if (!s[(int)nullIndex].GetType().Equals(typeof(NullSchema)))
                                nullIndex = 1L;
                            expressions = ResolveNullable(streamParameter, type, origin, nullableReadSchema, nullableWriterSchema, nullIndex);
                            break;
                        // Writer is an arbitrary Union
                        case UnionSchema s:
                            expressions = ResolveNullableFromUnion(streamParameter, nullableType, origin, nullableReadSchema, s);
                            break;
                        // Writer is not a Union nor a Null Type
                        default:
                            expressions = ResolveReader(origin, nullableType, nullableReadSchema, writerSchema, streamParameter);
                            if (nullableType.IsValueType)
                            {
                                expressions = new Tuple<Expression, Expression>(
                                    Expression.Convert(
                                        expressions.Item1,
                                        type
                                    ),
                                    expressions.Item2
                                );
                            }
                            break;
                    }
                    break;

                // Union: Reader is a Union but writer is not
                case UnionSchema r when type.Equals(typeof(object)) && !(writerSchema is UnionSchema):
                    var nonUnionToUnionIndex = FindMatch(writerSchema, r.ToArray(), out var nonUnionToUnionMatch);
                    var writeType = GetTypeFromSchema(nonUnionToUnionMatch, origin);
                    if (nonUnionToUnionIndex >= 0)
                    {
                        expressions = ResolveReader(origin, writeType, nonUnionToUnionMatch, writerSchema, streamParameter);
                        if (expressions != null)
                            expressions = new Tuple<Expression, Expression>(
                                Expression.Convert(
                                    expressions.Item1,
                                    type
                                ),
                                expressions.Item2
                            );
                    }
                    break;

                // Union: Reader is a Union and Writer is a Union
                case UnionSchema r when type.Equals(typeof(object)) && writerSchema is UnionSchema && (writerSchema as UnionSchema).Count > 0:
                    expressions = ResolveUnion(streamParameter, type, origin, r, (writerSchema as UnionSchema));
                    break;

                // Union Type to Single Type
                case Schema r when writerSchema is UnionSchema && (writerSchema as UnionSchema).Count > 0:
                    expressions = ResolveUnionToAny(streamParameter, type, origin, r, (writerSchema as UnionSchema));
                    break;
            }

            return expressions;
        }


        private static Tuple<Expression, Expression> ResolveNull(ParameterExpression streamParameter, Type type)
        {
            if (type.Equals(typeof(object)))
                return new Tuple<Expression, Expression>(
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.ReadNull))
                    ),
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.SkipNull))
                    )
                );
            else
                return new Tuple<Expression, Expression>(
                    Expression.Convert(
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadNull))
                        ),
                        type
                    ),
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.SkipNull))
                    )
                );
        }

        private static Tuple<Expression, Expression> ResolveBoolean(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadBoolean))
                        ),
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipBoolean))
                        )
                        );
        }
        private static Tuple<Expression, Expression> ResolveInt(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadInt))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipInt))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveLong(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadLong))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipLong))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveLongFromInt(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Convert(
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.ReadInt))
                    ),
                    typeof(long)
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipInt))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveFloat(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadFloat))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipFloat))
                )
            );
        }
        private static Tuple<Expression, Expression> ResolveFloatFromLong(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Convert(
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.ReadLong))
                    ),
                    typeof(float)
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipLong))
                )
            );
        }
        private static Tuple<Expression, Expression> ResolveFloatFromInt(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Convert(
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.ReadInt))
                    ),
                    typeof(float)
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipInt))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveDouble(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadDouble))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipDouble))
                )
            );
        }
        private static Tuple<Expression, Expression> ResolveDoubleFromFloat(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Convert(
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.ReadFloat))
                    ),
                    typeof(double)
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipFloat))
                )
            );
        }
        private static Tuple<Expression, Expression> ResolveDoubleFromLong(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Convert(
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.ReadLong))
                    ),
                    typeof(double)
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipLong))
                )
            );
        }
        private static Tuple<Expression, Expression> ResolveDoubleFromInt(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Convert(
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.ReadInt))
                    ),
                    typeof(double)
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipInt))
                )
            );
        }
        private static Tuple<Expression, Expression> ResolveBytes(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadBytes), Type.EmptyTypes)
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipBytes)),
                    null
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveString(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadString))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipString))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveUuid(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadUuid))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipUuid))
                )
            );
        }
        private static Tuple<Expression, Expression> ResolveDate(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadDate))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipDate))
                )
            );
        }
        private static Tuple<Expression, Expression> ResolveTimeMs(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimeMS))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimeMS))
                )
            );
        }
        private static Tuple<Expression, Expression> ResolveTimeUs(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimeUS))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimeUS))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveTimeUsFromMs(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimeMS))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimeMS))
                )
            );
        }
        private static Tuple<Expression, Expression> ResolveTimeNs(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimeNS))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimeNS))
                )
            );
        }
        private static Tuple<Expression, Expression> ResolveTimeNsFromUs(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimeUS))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimeUS))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveTimeNsFromMs(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimeMS))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimeMS))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveTimestampMs(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimestampMS))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimestampMS))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveTimestampUs(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimestampUS))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimestampUS))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveTimestampUsFromMs(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimestampMS))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimestampMS))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveTimestampNs(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimestampNS))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimestampNS))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveTimestampNsFromUs(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimestampUS))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimestampUS))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveTimestampNsFromMs(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimestampMS))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimestampMS))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveDuration(ParameterExpression streamParameter)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadDuration))
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipDuration))
                )
            );
        }
        private static Tuple<Expression, Expression> ResolveDecimal(ParameterExpression streamParameter, int scale)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadDecimal), new Type[] { typeof(int) }),
                    Expression.Constant(scale)
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipDecimal), new Type[] { })
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveDecimalFixed(ParameterExpression streamParameter, int scale, int size)
        {
            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadDecimal), new Type[] { typeof(int), typeof(int) }),
                    Expression.Constant(scale),
                    Expression.Constant(size)
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipDecimal), new Type[] { typeof(int) }),
                    Expression.Constant(size)
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveArray(ParameterExpression streamParameter, Type arrayItemType, Assembly origin, Schema readerItems, Schema writerItems)
        {
            var arrayItemReadFunction = typeof(Func<,>).MakeGenericType(typeof(IDecoder), arrayItemType);
            var arrayItemSkipAction = typeof(Action<>).MakeGenericType(typeof(IDecoder));

            var arrayItemExpressions = ResolveReader(origin, arrayItemType, readerItems, writerItems, streamParameter);

            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadArray)).MakeGenericMethod(arrayItemType),
                    Expression.Lambda(
                        arrayItemReadFunction,
                        arrayItemExpressions.Item1,
                        streamParameter
                    )
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipArray)),
                    Expression.Lambda(
                        arrayItemSkipAction,
                        arrayItemExpressions.Item2,
                        streamParameter
                    )
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveMap(ParameterExpression streamParameter, Type mapValueType, Assembly origin, Schema readerValues, Schema writerValues)
        {
            var mapValueReadFunction = typeof(Func<,>).MakeGenericType(typeof(IDecoder), mapValueType);
            var mapValueSkipAction = typeof(Action<>).MakeGenericType(typeof(IDecoder));

            var mapValuesExpressions = ResolveReader(origin, mapValueType, readerValues, writerValues, streamParameter);

            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadMap)).MakeGenericMethod(mapValueType),
                    Expression.Lambda(
                        mapValueReadFunction,
                        mapValuesExpressions.Item1,
                        streamParameter
                    )
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipMap)),
                    Expression.Lambda(
                        mapValueSkipAction,
                        mapValuesExpressions.Item2,
                        streamParameter
                    )
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveEnum(ParameterExpression streamParameter, Type enumType, IEnumerable<string> readerSymbols, IEnumerable<string> writerSymbols)
        {
            var errorSymbols = writerSymbols.Except(readerSymbols).ToHashSet();
            var switchCases = new SwitchCase[writerSymbols.Count()];

            var enumIndex =
                Expression.Variable(
                    typeof(int),
                    "enumIndex"
                );
            var enumValue =
                Expression.Variable(
                    enumType,
                    "enumValue"
                );

            for (int i = 0; i < writerSymbols.Count(); i++)
            {
                switchCases[i] =
                    Expression.SwitchCase(
                        Expression.Assign(
                            enumValue,
                            Expression.Constant(
                                Enum.Parse(enumType, writerSymbols.ElementAt(i)),
                                enumType
                            )
                        ),
                        Expression.Constant(
                            i,
                            typeof(int)
                        )
                    );
            }

            return new Tuple<Expression, Expression>(
                Expression.Block(
                    enumType,
                    new List<ParameterExpression>()
                    {
                enumIndex,
                enumValue
},
                    Expression.Assign(
                        enumIndex,
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadInt))
                        )
                    ),
                    Expression.Switch(
                        typeof(void),
                        enumIndex,
                        Expression.Throw(
                            Expression.Constant(
                                new IndexOutOfRangeException()
                            )
                        ),
                        null,
                        switchCases
                    ),
                    enumValue
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipInt))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveFixed(ParameterExpression streamParameter, Type fixedType, int size)
        {
            return new Tuple<Expression, Expression>(
                Expression.Block(
                    Expression.Convert(
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadFixed), new[] { typeof(int) }),
                            Expression.Constant(
                                size,
                                typeof(int)
                            )
                        ),
                        fixedType
                    )
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipFixed), new[] { typeof(int) }),
                    Expression.Constant(size, typeof(int))
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveRecord(ParameterExpression streamParameter, Type recordType, Assembly origin, IEnumerable<RecordSchema.Field> readerFields, IEnumerable<RecordSchema.Field> writerFields)
        {
            var missingFieldMap = readerFields.Where(f => !writerFields.Any(w => w.Name == f.Name));
            var missingDefaults = missingFieldMap.Where(f => f.Default == null);
            if (missingDefaults.Count() > 0)
                throw new AvroException($"Unmapped field without default: '{string.Join(", ", missingDefaults.Select(f => f.Name))}'");

            var fieldReaders = new List<Expression>();
            var fieldSkippers = new List<Expression>();

            var recordParameter =
                Expression.Variable(
                    recordType,
                    "record"
                );

            fieldReaders.Add(
                Expression.Assign(
                    recordParameter,
                    Expression.New(recordType)
                )
            );

            foreach (var writerField in writerFields)
            {
                var readerField = readerFields.FirstOrDefault(f => f.Name == writerField.Name);
                var fieldExpressions = default(Tuple<Expression, Expression>);

                if (readerField == null)
                {
                    fieldExpressions = ResolveReader(origin, GetTypeFromSchema(writerField.Type, origin), writerField.Type, writerField.Type, streamParameter);
                    fieldReaders.Add(
                        fieldExpressions.Item2
                    );
                    fieldSkippers.Add(
                        fieldExpressions.Item2
                    );
                }
                else
                {
                    fieldExpressions = ResolveReader(origin, GetTypeFromSchema(readerField.Type, origin), readerField.Type, writerField.Type, streamParameter);
                    fieldReaders.Add(
                        Expression.Assign(
                            Expression.MakeMemberAccess
                            (
                                recordParameter,
                                recordType.GetProperty(readerField.Name)
                            ),
                            fieldExpressions.Item1
                        )
                    );
                    fieldSkippers.Add(
                        fieldExpressions.Item2
                    );
                }
            }

            // Append Record parameter in order to return it.
            fieldReaders.Add(recordParameter);

            return new Tuple<Expression, Expression>(
                Expression.Block(
                    recordType,
                    new ParameterExpression[] { recordParameter },
                    fieldReaders
                ),
                Expression.Block(
                    fieldSkippers
                )
            );
        }


        private static Tuple<Expression, Expression> ResolveNullable(ParameterExpression streamParameter, Type nullableType, Assembly origin, Schema readSchema, Schema writeSchema, long nullIndex)
        {
            var valueType = Nullable.GetUnderlyingType(nullableType) ?? nullableType;
            var valueExpressions = ResolveReader(origin, valueType, readSchema, writeSchema, streamParameter);

            var nullableValueMethod =
                valueType.IsClass || valueType.IsInterface ?
                typeof(IDecoder).GetMethod(nameof(IDecoder.ReadNullableObject)).MakeGenericMethod(valueType) :
                typeof(IDecoder).GetMethod(nameof(IDecoder.ReadNullableValue)).MakeGenericMethod(valueType)
            ;

            return new Tuple<Expression, Expression>(
                Expression.Call(
                    streamParameter,
                    nullableValueMethod,
                    Expression.Lambda(
                        typeof(Func<,>).MakeGenericType(typeof(IDecoder), valueType),
                        valueExpressions.Item1,
                        streamParameter
                    ),
                    Expression.Constant(
                        nullIndex,
                        typeof(long)
                    )
                ),
                Expression.Call(
                    streamParameter,
                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipNullable)),
                    Expression.Lambda(
                        typeof(Action<>).MakeGenericType(typeof(IDecoder)),
                        valueExpressions.Item1,
                        streamParameter
                    ),
                    Expression.Constant(
                        nullIndex,
                        typeof(long)
                    )
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveNullableFromUnion(ParameterExpression streamParameter, Type nullableType, Assembly origin, Schema readSchema, IEnumerable<Schema> writeSchemas)
        {
            var valueType = Nullable.GetUnderlyingType(nullableType) ?? nullableType;
            var unionReadSwitchCases = new List<SwitchCase>();
            var unionSkipSwitchCases = new List<SwitchCase>();
            var unionTypeIndex =
                Expression.Variable(
                    typeof(long),
                    "unionTypeIndex"
                );
            var unionTypeValue =
                Expression.Variable(
                    valueType,
                    "unionTypeValue"
                );

            for (int i = 0; i < writeSchemas.Count(); i++)
            {
                var unionExpressions = default(Tuple<Expression, Expression>);

                if (writeSchemas.ElementAt(i) is NullSchema)
                    unionExpressions = ResolveNull(streamParameter, typeof(object));
                else
                    unionExpressions = ResolveReader(origin, valueType, readSchema, writeSchemas.ElementAt(i), streamParameter);

                if (unionExpressions != null)
                {
                    unionExpressions = new Tuple<Expression, Expression>(
                        Expression.Assign(
                            unionTypeValue,
                            Expression.Convert(
                                unionExpressions.Item1,
                                nullableType
                            )
                        ),
                        unionExpressions.Item2
                    );
                }
                else
                {
                    unionExpressions = ResolveReader(origin, GetTypeFromSchema(writeSchemas.ElementAt(i), origin), writeSchemas.ElementAt(i), writeSchemas.ElementAt(i), streamParameter);

                    unionExpressions = new Tuple<Expression, Expression>(
                        Expression.Throw(
                            Expression.Constant(
                                new InvalidCastException()
                            )
                        ) as Expression,
                        unionExpressions.Item2
                    );
                }

                unionReadSwitchCases.Add(
                    Expression.SwitchCase(
                        unionExpressions.Item1,
                        Expression.Constant(
                            (long)i,
                            typeof(long)
                        )
                    )
                );

                unionSkipSwitchCases.Add(
                    Expression.SwitchCase(
                        unionExpressions.Item2,
                        Expression.Constant(
                            (long)i,
                            typeof(long)
                        )
                    )
                );
            }

            return new Tuple<Expression, Expression>(
                Expression.Block(
                    nullableType,
                    new List<ParameterExpression>()
                    {
                        unionTypeIndex,
                        unionTypeValue
                    },
                    Expression.Assign(
                        unionTypeIndex,
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadLong))
                        )
                    ),
                    Expression.Switch(
                        typeof(void),
                        unionTypeIndex,
                        Expression.Throw(
                            Expression.Constant(
                                new IndexOutOfRangeException()
                            )
                        ),
                        null,
                        unionReadSwitchCases
                    ),
                    unionTypeValue
                ),
                Expression.Block(
                    typeof(void),
                    new List<ParameterExpression>()
                    {
                        unionTypeIndex
                    },
                    Expression.Assign(
                        unionTypeIndex,
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadLong))
                        )
                    ),
                    Expression.Switch(
                        typeof(void),
                        unionTypeIndex,
                        Expression.Throw(
                            Expression.Constant(
                                new IndexOutOfRangeException()
                            )
                        ),
                        null,
                        unionSkipSwitchCases
                    )
                )
            );
        }

        private static Tuple<Expression, Expression> ResolveUnion(ParameterExpression streamParameter, Type type, Assembly origin, IEnumerable<Schema> readSchemas, IEnumerable<Schema> writeSchemas)
        {
            var unionReadSwitchCases = new SwitchCase[writeSchemas.Count()];
            var unionSkipSwitchCases = new SwitchCase[writeSchemas.Count()];
            var unionToUnionTypeIndex =
                Expression.Variable(
                    typeof(long),
                    "unionTypeIndex"
                );
            var unionTypeValue =
                Expression.Variable(
                    typeof(object),
                    "unionTypeValue"
                );
            for (int i = 0; i < writeSchemas.Count(); i++)
            {
                var unionExpressions = default(Tuple<Expression, Expression>);
                var unionToUnionIndex = FindMatch(writeSchemas.ElementAt(i), readSchemas.ToArray(), out var unionToUnionMatch);
                if (unionToUnionIndex >= 0)
                    unionExpressions = ResolveReader(origin, GetTypeFromSchema(unionToUnionMatch, origin), unionToUnionMatch, writeSchemas.ElementAt(i), streamParameter);

                if (unionExpressions != null)
                {
                    unionExpressions = new Tuple<Expression, Expression>(
                        Expression.Assign(
                            unionTypeValue,
                            Expression.Convert(
                                unionExpressions.Item1,
                                type
                            )
                        ),
                        unionExpressions.Item2
                    );
                }
                else
                {
                    unionExpressions = ResolveReader(origin, GetTypeFromSchema(writeSchemas.ElementAt(i), origin), writeSchemas.ElementAt(i), writeSchemas.ElementAt(i), streamParameter);

                    unionExpressions = new Tuple<Expression, Expression>(
                        Expression.Throw(
                            Expression.Constant(
                                new InvalidCastException()
                            )
                        ) as Expression,
                        unionExpressions.Item2
                    );
                }

                unionReadSwitchCases[i] =
                    Expression.SwitchCase(
                        unionExpressions.Item1,
                        Expression.Constant(
                            (long)i,
                            typeof(long)
                        )
                    );

                unionSkipSwitchCases[i] =
                    Expression.SwitchCase(
                        unionExpressions.Item2,
                        Expression.Constant(
                            (long)i,
                            typeof(long)
                        )
                    );
            }

            return new Tuple<Expression, Expression>(
            Expression.Block(
                type,
                new List<ParameterExpression>()
                {
                        unionToUnionTypeIndex,
                        unionTypeValue
                },
                Expression.Assign(
                    unionToUnionTypeIndex,
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.ReadLong))
                    )
                ),
                Expression.Switch(
                    typeof(void),
                    unionToUnionTypeIndex,
                    Expression.Throw(
                        Expression.Constant(
                            new IndexOutOfRangeException()
                        )
                    ),
                    null,
                    unionReadSwitchCases
                ),
                unionTypeValue
            ),
            Expression.Block(
                new List<ParameterExpression>()
                {
                        unionToUnionTypeIndex
                },
                Expression.Assign(
                    unionToUnionTypeIndex,
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.ReadLong))
                    )
                ),
                Expression.Switch(
                    typeof(void),
                    unionToUnionTypeIndex,
                    Expression.Throw(
                        Expression.Constant(
                            new IndexOutOfRangeException()
                        )
                    ),
                    null,
                    unionSkipSwitchCases
                )
            )
        );
        }

        private static Tuple<Expression, Expression> ResolveUnionToAny(ParameterExpression streamParameter, Type type, Assembly origin, Schema readSchema, IEnumerable<Schema> writeSchemas)
        {
            var unionToNonUnionReadCases = new SwitchCase[writeSchemas.Count()];
            var unionToNonUnionSkipCases = new SwitchCase[writeSchemas.Count()];
            var unionToNonUnionTypeIndex =
                Expression.Variable(
                    typeof(long),
                    "unionTypeIndex"
                );
            var unionToNonUnionTypeValue =
                Expression.Variable(
                    type,
                    "unionTypeValue"
                );

            for (int i = 0; i < writeSchemas.Count(); i++)
            {
                var unionExpressions = ResolveReader(origin, type, readSchema, writeSchemas.ElementAt(i), streamParameter);
                if (unionExpressions != null)
                {
                    unionToNonUnionReadCases[i] =
                        Expression.SwitchCase(
                            Expression.Assign(
                                unionToNonUnionTypeValue,
                                Expression.Convert(
                                    unionExpressions.Item1,
                                    type
                                )
                            ),
                            Expression.Constant(
                                (long)i,
                                typeof(long)
                            )
                        );
                }
                else
                {
                    unionToNonUnionReadCases[i] =
                        Expression.SwitchCase(
                            Expression.Throw(
                                Expression.Constant(
                                    new InvalidCastException()
                                )
                            ),
                            Expression.Constant(
                                (long)i,
                                typeof(long)
                            )
                        );
                    unionExpressions = ResolveReader(origin, GetTypeFromSchema(writeSchemas.ElementAt(i), origin), writeSchemas.ElementAt(i), writeSchemas.ElementAt(i), streamParameter);
                }

                unionToNonUnionSkipCases[i] =
                    Expression.SwitchCase(
                        unionExpressions.Item2,
                        Expression.Constant(
                            (long)i,
                            typeof(long)
                        )
                    );
            }

            return new Tuple<Expression, Expression>(
                Expression.Block(
                    type,
                    new List<ParameterExpression>()
                    {
                        unionToNonUnionTypeIndex,
                        unionToNonUnionTypeValue
                    },
                    Expression.Assign(
                        unionToNonUnionTypeIndex,
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadLong))
                        )
                    ),
                    Expression.Switch(
                        typeof(void),
                        unionToNonUnionTypeIndex,
                        Expression.Throw(
                            Expression.Constant(
                                new IndexOutOfRangeException()
                            )
                        ),
                        null,
                        unionToNonUnionReadCases
                    ),
                    unionToNonUnionTypeValue
                ),
                Expression.Block(
                    new List<ParameterExpression>()
                    {
                                unionToNonUnionTypeIndex
                    },
                    Expression.Assign(
                        unionToNonUnionTypeIndex,
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadLong))
                        )
                    ),
                    Expression.Switch(
                        typeof(void),
                        unionToNonUnionTypeIndex,
                        Expression.Throw(
                            Expression.Constant(
                                new IndexOutOfRangeException()
                            )
                        ),
                        null,
                        unionToNonUnionSkipCases
                    )
                )
            );
        }
    }
}
