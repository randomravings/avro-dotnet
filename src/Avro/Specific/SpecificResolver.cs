using Avro.IO;
using Avro.Schemas;
using Avro.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Avro.Specific
{
    public static class SpecificResolver
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

        public static Action<IEncoder, T> ResolveWriter<T>(Schema writerSchema)
        {
            var type = typeof(T);
            var streamParameter = Expression.Parameter(typeof(IEncoder), "s");
            var valueParameter = Expression.Parameter(type, "v");
            var writeAction = typeof(Action<,>).MakeGenericType(typeof(IEncoder), type);
            if (!ResolveWriter(type.Assembly, type, writerSchema, streamParameter, valueParameter, new Stack<PropertyInfo>(), null, out var writeExpression))
                throw new AvroException($"Unable to resolve writer: '{writerSchema}' for type: '{type}'");


            return
                Expression.Lambda(
                    writeAction,
                    writeExpression,
                    streamParameter,
                    valueParameter
                )
                .Compile() as Action<IEncoder, T>;
        }

        private static bool ResolveWriter(Assembly origin, Type type, Schema writerSchema, ParameterExpression streamParameter, ParameterExpression valueParameter, Stack<PropertyInfo> propertyChain, Type valueParameterCast, out Expression writeExpression)
        {
            var valueParameterOrProperty = GetValueExpression(valueParameter, propertyChain);
            writeExpression = null;

            switch (writerSchema)
            {
                case NullSchema r when type.IsClass || (type.IsValueType && Nullable.GetUnderlyingType(type) != null):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteNull))
                    );
                    break;

                case BooleanSchema r when type.Equals(typeof(bool)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteBoolean)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case IntSchema r when type.Equals(typeof(int)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteInt)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case LongSchema r when type.Equals(typeof(long)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteLong)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case FloatSchema r when type.Equals(typeof(float)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteFloat)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case DoubleSchema r when type.Equals(typeof(double)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteDouble)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case BytesSchema r when type.Equals(typeof(byte[])):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteBytes)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case StringSchema r when type.Equals(typeof(string)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteString)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case UuidSchema r when type.Equals(typeof(Guid)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteUuid)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case DateSchema r when type.Equals(typeof(DateTime)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteDate)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case TimeMillisSchema r when type.Equals(typeof(TimeSpan)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteTimeMS)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case TimeMicrosSchema r when type.Equals(typeof(TimeSpan)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteTimeUS)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case TimeNanosSchema r when type.Equals(typeof(TimeSpan)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteTimeNS)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case TimestampMillisSchema r when type.Equals(typeof(DateTime)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteTimestampMS)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case TimestampMicrosSchema r when type.Equals(typeof(DateTime)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteTimestampUS)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case TimestampNanosSchema r when type.Equals(typeof(DateTime)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteTimestampNS)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case DurationSchema r when type.Equals(typeof(Tuple<int, int, int>)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteDuration)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast)
                    );
                    break;

                case DecimalSchema r when type.Equals(typeof(decimal)):
                    switch (r.Type)
                    {
                        case BytesSchema t:
                            writeExpression = Expression.Call(
                                streamParameter,
                                typeof(IEncoder).GetMethod(nameof(IEncoder.WriteDecimal)),
                                CastOrExpression(valueParameterOrProperty, valueParameterCast)
                            );
                            break;
                        case FixedSchema t:
                            writeExpression = Expression.Call(
                                streamParameter,
                                typeof(IEncoder).GetMethod(nameof(IEncoder.WriteDecimal)),
                                CastOrExpression(valueParameterOrProperty, valueParameterCast),
                                Expression.Constant(r.Scale, typeof(int))
                            );
                            break;
                    }
                    break;

                case ArraySchema r when type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IList<>):
                    ResolveWriter(origin, type.GenericTypeArguments.Last(), r.Items, streamParameter, valueParameter, propertyChain, null, out var arrayItemExpression);
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteArray)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast),
                        arrayItemExpression
                    );
                    break;

                case MapSchema r when type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IDictionary<,>):
                    ResolveWriter(origin, type.GenericTypeArguments.Last(), r.Values, streamParameter, valueParameter, propertyChain, null, out var mapItemExpression);
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteArray)),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast),
                        mapItemExpression
                    );
                    break;

                case EnumSchema r when type.IsEnum && (Enum.GetNames(type).Intersect(r.Symbols).Count() == r.Symbols.Count):
                    var switchCases = new SwitchCase[r.Symbols.Count];
                    for (int i = 0; i < r.Symbols.Count; i++)
                    {
                        switchCases[i] =
                            Expression.SwitchCase(
                                Expression.Call(
                                    streamParameter,
                                    typeof(IEncoder).GetMethod(nameof(IEncoder.WriteInt)),
                                    Expression.Constant(i, typeof(int))
                                ),
                                Expression.Constant(
                                    Enum.Parse(type, r.Symbols[i])
                                )
                            );
                    }
                    writeExpression =
                        Expression.Switch(
                            CastOrExpression(valueParameterOrProperty, valueParameterCast),
                            switchCases
                        );
                    break;

                case FixedSchema r when typeof(ISpecificFixed).IsAssignableFrom(type):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteFixed)),
                        CastOrExpression(Expression.MakeMemberAccess(valueParameterOrProperty, typeof(ISpecificFixed).GetProperty(nameof(ISpecificFixed.Value))), valueParameterCast)
                    );
                    break;

                case RecordSchema r when typeof(ISpecificRecord).IsAssignableFrom(type):
                    var fieldExpressions = new List<Expression>();
                    foreach (var field in r)
                    {
                        var recordProperty = type.GetProperty(field.Name);
                        var fieldValueExpression =
                            Expression.MakeMemberAccess(
                                valueParameterOrProperty,
                                recordProperty
                            );
                        propertyChain.Push(recordProperty);
                        ResolveWriter(origin, recordProperty.PropertyType, field.Type, streamParameter, valueParameter, propertyChain, null, out var fieldExpression);
                        fieldExpressions.Add(fieldExpression);
                        propertyChain.Pop();
                    }
                    writeExpression = Expression.Block(fieldExpressions);
                    break;

                case UnionSchema r when (Nullable.GetUnderlyingType(type) != null || type.IsClass) && r.Count == 2 && r.FirstOrDefault(n => n.GetType().Equals(typeof(NullSchema))) != null:
                    var localType = type;
                    var nullIndex = 0;
                    if (!r[nullIndex].GetType().Equals(typeof(NullSchema)))
                        nullIndex = 1;
                    if (Nullable.GetUnderlyingType(type) != null)
                    {
                        localType = Nullable.GetUnderlyingType(type);
                        propertyChain.Push(type.GetProperty("Value"));
                    }
                    ResolveWriter(origin, localType, r[(nullIndex + 1) % 2], streamParameter, valueParameter, propertyChain, null, out var writeNotNullExpression);
                    writeExpression =
                        Expression.IfThenElse
                        (
                            Expression.Equal(
                                valueParameter,
                                Expression.Constant(null, type)
                            ),
                            Expression.Call(
                                streamParameter,
                                typeof(IEncoder).GetMethod(nameof(IEncoder.WriteLong)),
                                Expression.Constant(
                                    (long)nullIndex,
                                    typeof(long)
                                )
                            ),
                            Expression.Block(
                                typeof(void),
                                Expression.Call(
                                    streamParameter,
                                    typeof(IEncoder).GetMethod(nameof(IEncoder.WriteLong)),
                                    Expression.Constant(
                                        (long)(nullIndex + 1) % 2,
                                        typeof(long)
                                    )
                                ),
                                writeNotNullExpression
                            )
                        );
                    if (Nullable.GetUnderlyingType(type) != null)
                        propertyChain.Pop();
                    break;

                case UnionSchema r when type.Equals(typeof(object)) && r.Count > 0:
                    writeExpression =
                        Expression.Throw(
                            Expression.Constant(new ArgumentException())
                        ) as Expression;
                    for (int i = 0; i < r.Count; i++)
                    {
                        var unionSubType = SchemaToType.Get(r[i], origin);
                        ResolveWriter(origin, unionSubType, r[i], streamParameter, valueParameter, propertyChain, unionSubType, out var unionSubExpression);
                        writeExpression =
                            Expression.IfThenElse((
                                r[i] is NullSchema ?
                                Expression.Equal(
                                    valueParameterOrProperty,
                                    Expression.Constant(
                                        null,
                                        typeof(object)
                                    )
                                ) as Expression :
                                Expression.TypeIs(
                                    valueParameterOrProperty,
                                    unionSubType
                                ) as Expression),
                                Expression.Block(
                                    typeof(void),
                                    Expression.Call(
                                        streamParameter,
                                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteLong)),
                                        Expression.Constant(
                                            (long)i,
                                            typeof(long)
                                        )
                                    ),
                                    unionSubExpression
                                ),
                                writeExpression
                            );
                    }
                    break;
            }

            return writeExpression != null;
        }

        public static Tuple<Func<IDecoder, T>, Action<IDecoder>> ResolveReader<T>(Schema readerSchema, Schema writerSchema)
        {
            var type = typeof(T);
            var readFunction = typeof(Func<,>).MakeGenericType(typeof(IDecoder), type);
            var skipAction = typeof(Action<>).MakeGenericType(typeof(IDecoder));
            var streamParameter = Expression.Parameter(typeof(IDecoder), "s");

            if (!ResolveReader(type.Assembly, type, readerSchema, writerSchema, streamParameter, null, new Stack<PropertyInfo>(), out var readExpression, out var skipExpression))
                throw new AvroException($"Unable to resolve reader: '{readerSchema}' using writer: '{writerSchema}' for type: '{type}'");

            var readLambdaExpression =
                Expression.Lambda(
                    readFunction,
                    readExpression,
                    streamParameter
                )
                .Compile() as Func<IDecoder, T>;

            var skipLambdaExpression =
                Expression.Lambda(
                    skipAction,
                    skipExpression,
                    streamParameter
                )
                .Compile() as Action<IDecoder>;

            return new Tuple<Func<IDecoder, T>, Action<IDecoder>>(
                readLambdaExpression,
                skipLambdaExpression
            );
        }

        private static bool ResolveReader(Assembly origin, Type type, Schema readerSchema, Schema writerSchema, ParameterExpression streamParameter, ParameterExpression valueParameter, Stack<PropertyInfo> propertyChain, out Expression readExpression, out Expression skipExpression)
        {
            var assign = valueParameter != null;
            readExpression = null;
            skipExpression = null;

            switch (readerSchema)
            {
                case NullSchema r when writerSchema is NullSchema && (type.IsClass || (type.IsValueType && Nullable.GetUnderlyingType(type) != null)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadNull))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipNull))
                        );
                    break;
                case BooleanSchema r when writerSchema is BooleanSchema && type.Equals(typeof(bool)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadBoolean))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipBoolean))
                        );
                    break;
                case IntSchema r when writerSchema is IntSchema && type.Equals(typeof(int)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadInt))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipInt))
                        );
                    break;
                case LongSchema r when writerSchema is LongSchema && type.Equals(typeof(long)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadLong))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipLong))
                        );
                    break;
                case LongSchema r when writerSchema is IntSchema && type.Equals(typeof(long)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadInt))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipInt))
                        );
                    break;
                case FloatSchema r when writerSchema is FloatSchema && type.Equals(typeof(float)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadFloat))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipFloat))
                        );
                    break;
                case FloatSchema r when writerSchema is LongSchema && type.Equals(typeof(float)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadLong))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipLong))
                        );
                    break;
                case FloatSchema r when writerSchema is IntSchema && type.Equals(typeof(float)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadInt))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipInt))
                        );
                    break;
                case DoubleSchema r when writerSchema is DoubleSchema && type.Equals(typeof(double)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadDouble))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipDouble))
                        );
                    break;
                case DoubleSchema r when writerSchema is FloatSchema && type.Equals(typeof(double)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadFloat))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipFloat))
                        );
                    break;
                case DoubleSchema r when writerSchema is LongSchema && type.Equals(typeof(double)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadLong))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipLong))
                        );
                    break;
                case DoubleSchema r when writerSchema is IntSchema && type.Equals(typeof(double)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadInt))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipInt))
                        );
                    break;
                case BytesSchema r when writerSchema is BytesSchema && type.Equals(typeof(byte[])):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadBytes), Type.EmptyTypes)
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipBytes)),
                            null
                        );
                    break;
                case BytesSchema r when writerSchema is StringSchema && type.Equals(typeof(byte[])):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadBytes), Type.EmptyTypes)
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipBytes))
                        );
                    break;
                case StringSchema r when writerSchema is StringSchema && type.Equals(typeof(string)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadString))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipString))
                        );
                    break;
                case StringSchema r when writerSchema is BytesSchema && type.Equals(typeof(string)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadString))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipString))
                        );
                    break;
                case UuidSchema r when writerSchema is UuidSchema && type.Equals(typeof(Guid)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadUuid))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipUuid))
                        );
                    break;
                case DateSchema r when writerSchema is DateSchema && type.Equals(typeof(DateTime)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadDate))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipDate))
                        );
                    break;
                case TimeMillisSchema r when writerSchema is TimeMillisSchema && type.Equals(typeof(TimeSpan)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimeMS))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimeMS))
                        );
                    break;
                case TimeMicrosSchema r when writerSchema is TimeMicrosSchema && type.Equals(typeof(TimeSpan)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimeUS))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimeUS))
                        );
                    break;
                case TimeMicrosSchema r when writerSchema is TimeMillisSchema && type.Equals(typeof(TimeSpan)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimeMS))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimeMS))
                        );
                    break;
                case TimeNanosSchema r when writerSchema is TimeNanosSchema && type.Equals(typeof(TimeSpan)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimeNS))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimeNS))
                        );
                    break;
                case TimeNanosSchema r when writerSchema is TimeMicrosSchema && type.Equals(typeof(TimeSpan)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimeUS))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimeUS))
                        );
                    break;
                case TimeNanosSchema r when writerSchema is TimeMillisSchema && type.Equals(typeof(TimeSpan)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimeMS))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimeMS))
                        );
                    break;
                case TimestampMillisSchema r when writerSchema is TimestampMillisSchema && type.Equals(typeof(DateTime)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimestampMS))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimestampMS))
                        );
                    break;
                case TimestampMicrosSchema r when writerSchema is TimestampMicrosSchema && type.Equals(typeof(DateTime)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimestampUS))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimestampUS))
                        );
                    break;
                case TimestampMicrosSchema r when writerSchema is TimestampMillisSchema && type.Equals(typeof(DateTime)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimestampMS))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimestampMS))
                        );
                    break;
                case TimestampNanosSchema r when writerSchema is TimestampNanosSchema && type.Equals(typeof(DateTime)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimestampNS))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimestampNS))
                        );
                    break;
                case TimestampNanosSchema r when writerSchema is TimestampMicrosSchema && type.Equals(typeof(DateTime)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimestampUS))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimestampUS))
                        );
                    break;
                case TimestampNanosSchema r when writerSchema is TimestampMillisSchema && type.Equals(typeof(DateTime)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadTimestampMS))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipTimestampMS))
                        );
                    break;
                case DurationSchema r when writerSchema is DurationSchema && type.Equals(typeof(Tuple<int, int, int>)):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadDuration))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipDuration))
                        );
                    break;
                case DecimalSchema r when writerSchema is DecimalSchema && (writerSchema as DecimalSchema).Equals(r):
                    switch (r.Type)
                    {
                        case BytesSchema t:
                            readExpression =
                                Expression.Call(
                                    streamParameter,
                                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadDecimal))
                                );
                            skipExpression =
                                Expression.Call(
                                    streamParameter,
                                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipDecimal))
                                );
                            break;
                        case FixedSchema t:
                            readExpression =
                                Expression.Call(
                                    streamParameter,
                                    typeof(IDecoder).GetMethod(nameof(IDecoder.ReadDecimal))
                                );
                            skipExpression =
                                Expression.Call(
                                    streamParameter,
                                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipDecimal)),
                                    Expression.Constant(t.Size)
                                );
                            break;
                    }
                    break;
                case ArraySchema r when writerSchema is ArraySchema && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IList<>):
                    var arrayType = type.GenericTypeArguments.Last();
                    ResolveReader(origin, arrayType, r.Items, (writerSchema as ArraySchema).Items, streamParameter, valueParameter, propertyChain, out var arrayItemReadExpression, out var arrayItemSkipExpression);
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadArray)),
                            arrayItemReadExpression
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipArray)),
                            arrayItemSkipExpression
                        );
                    break;

                case MapSchema r when writerSchema is MapSchema && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IDictionary<,>):
                    var mapType = type.GenericTypeArguments.Last();
                    ResolveReader(origin, mapType, r.Values, (writerSchema as MapSchema).Values, streamParameter, valueParameter, propertyChain, out var mapValueReadExpression, out var mapValueSkipExpression);
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadMap)),
                            mapValueReadExpression
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipMap)),
                            mapValueSkipExpression
                        );
                    break;

                case EnumSchema r when writerSchema is EnumSchema && r.Equals(writerSchema) && type.IsEnum && Enum.GetNames(type).Intersect(r.Symbols).Count() == r.Symbols.Count:
                    var writerEnumSchema = writerSchema as EnumSchema;
                    var errorSymbols = writerEnumSchema.Symbols.Except(r.Symbols).ToHashSet();
                    var switchCases = new SwitchCase[writerEnumSchema.Symbols.Count];

                    var enumIndex =
                        Expression.Variable(
                            typeof(int),
                            "enumIndex"
                        );
                    var enumValue =
                        Expression.Variable(
                            type,
                            "enumValue"
                        );

                    for (int i = 0; i < writerEnumSchema.Symbols.Count; i++)
                    {
                        switchCases[i] =
                            Expression.SwitchCase(
                                Expression.Assign(
                                    enumValue,
                                    Expression.Constant(
                                        Enum.Parse(type, writerEnumSchema.Symbols[i]),
                                        type
                                    )
                                ),
                                Expression.Constant(
                                    i,
                                    typeof(int)
                                )
                            );
                    }

                    readExpression =
                        Expression.Block(
                            type,
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
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipInt))
                        );
                    break;

                case FixedSchema r when writerSchema is FixedSchema && r.Equals(writerSchema) && typeof(ISpecificFixed).IsAssignableFrom(type):
                    readExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.ReadFixed)),
                            Expression.Constant(r.Size, typeof(int))
                        );
                    skipExpression =
                        Expression.Call(
                            streamParameter,
                            typeof(IDecoder).GetMethod(nameof(IDecoder.SkipFixed)),
                            Expression.Constant(r.Size, typeof(int))
                        );
                    break;

                case RecordSchema r when writerSchema is RecordSchema && r.Equals(writerSchema) && typeof(ISpecificRecord).IsAssignableFrom(type):
                    var recordWriterSchema = writerSchema as RecordSchema;

                    var missingFieldMap = r.Where(f => !recordWriterSchema.Any(w => w.Name == f.Name)).ToDictionary(k => k.Name);
                    var missingDefaults = missingFieldMap.Values.Where(f => f.Default == null);
                    if (missingDefaults.Count() > 0)
                        throw new AvroException($"Unmapped field without default: '{string.Join(", ", missingDefaults.Select(f => f.Name))}'");

                    var fieldReaders = new List<Expression>();
                    var fieldSkippers = new List<Expression>();
                    var recordParameters = new List<ParameterExpression>();

                    var recordParameter =
                        valueParameter ??
                            Expression.Variable(
                                type,
                                "record"
                            );

                    if (valueParameter == null)
                        recordParameters.Add(
                            recordParameter
                        );

                    var valueExpression = GetValueExpression(recordParameter, propertyChain);

                    fieldReaders.Add(
                        Expression.Assign(
                            valueExpression,
                            Expression.New(type)
                        )
                    );

                    // TODO: Create expression for record.<Field> = default;
                    // This assumes that auto property with defaults are coded as <FieldName>{ get; set;} = <default_value>;
                    foreach (var defaultField in missingFieldMap.Values) { }

                    foreach (var writerField in recordWriterSchema)
                    {
                        var readerField = r.FirstOrDefault(f => f.Name == writerField.Name);

                        if (readerField == null)
                        {
                            ResolveReader(origin, SchemaToType.Get(writerField.Type, origin), writerField.Type, writerField.Type, streamParameter, recordParameter, propertyChain, out _, out var fieldSkipExpression);
                            fieldReaders.Add(
                                fieldSkipExpression
                            );
                            fieldSkippers.Add(
                                fieldSkipExpression
                            );
                        }
                        else
                        {
                            propertyChain.Push(type.GetProperty(readerField.Name));
                            ResolveReader(origin, SchemaToType.Get(readerField.Type, origin), readerField.Type, writerField.Type, streamParameter, recordParameter, propertyChain, out var fieldReadExpression, out var fieldSkipExpression);
                            fieldReaders.Add(
                                fieldReadExpression
                            );
                            fieldSkippers.Add(
                                fieldSkipExpression
                            );
                            propertyChain.Pop();
                        }
                    }

                    if (valueParameter == null)
                        fieldReaders.Add(
                            recordParameter
                        );

                    readExpression =
                        Expression.Block(
                            (valueParameter == null ? type : typeof(void)),
                            recordParameters,
                            fieldReaders
                        );

                    skipExpression =
                        Expression.Block(
                            fieldSkippers
                        );

                    assign = false;
                    break;

                case UnionSchema r when (Nullable.GetUnderlyingType(type) != null || type.IsClass) && r.Count == 2 && r.FirstOrDefault(n => n.GetType().Equals(typeof(NullSchema))) != null:
                    var localReaderSchema = r.FirstOrDefault(n => !n.GetType().Equals(typeof(NullSchema)));
                    var localReaderType = Nullable.GetUnderlyingType(type) ?? type;
                    switch (writerSchema)
                    {
                        case NullSchema s:
                            readExpression =
                                Expression.Call(
                                    streamParameter,
                                     typeof(IDecoder).GetMethod(nameof(IDecoder.ReadNull))
                                );
                            skipExpression =
                                Expression.Call(
                                    streamParameter,
                                     typeof(IDecoder).GetMethod(nameof(IDecoder.SkipNull))
                                );
                            break;
                        case UnionSchema s when s.Count == 2 && s.FirstOrDefault(n => n.GetType().Equals(typeof(NullSchema))) != null:
                            var localWriterSchema = s.FirstOrDefault(n => !n.GetType().Equals(typeof(NullSchema)));
                            var nullIndex = 0;
                            if (!s[nullIndex].GetType().Equals(typeof(NullSchema)))
                                nullIndex = 1;

                            ResolveReader(origin, localReaderType, localReaderSchema, localWriterSchema, streamParameter, valueParameter, propertyChain, out var nullableValueReadExpression, out _);

                            var nullableValueMethod =
                                type.IsClass ?
                                typeof(IDecoder).GetMethod(nameof(IDecoder.ReadNullableObject)).MakeGenericMethod(localReaderType) :
                                typeof(IDecoder).GetMethod(nameof(IDecoder.ReadNullableValue)).MakeGenericMethod(localReaderType)
                            ;

                            readExpression =
                                Expression.Call(
                                    streamParameter,
                                    nullableValueMethod,
                                    Expression.Lambda(
                                        typeof(Func<,>).MakeGenericType(typeof(IDecoder), localReaderType),
                                        nullableValueReadExpression,
                                        streamParameter
                                    ),
                                    Expression.Constant(
                                        (long)nullIndex,
                                        typeof(long)
                                    )
                                );

                            skipExpression =
                                Expression.Call(
                                    streamParameter,
                                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipNullable)),
                                    Expression.Lambda(
                                        typeof(Action<>).MakeGenericType(typeof(IDecoder)),
                                        nullableValueReadExpression,
                                        streamParameter
                                    ),
                                    Expression.Constant(
                                        (long)nullIndex,
                                        typeof(long)
                                    )
                                );
                            break;
                        case UnionSchema s:
                            var unionSwitchCases = new List<SwitchCase>();
                            var unionTypeIndex =
                                Expression.Variable(
                                    typeof(long),
                                    "unionTypeIndex"
                                );
                            var unionTypeValue =
                                Expression.Variable(
                                    type,
                                    "unionTypeValue"
                                );

                            for (int i = 0; i < s.Count; i++)
                            {
                                var unionCaseExpression =
                                    Expression.Throw(
                                        Expression.Constant(
                                            new InvalidCastException()
                                        )
                                    ) as Expression;

                                if (ResolveReader(origin, type, readerSchema, s[i], streamParameter, valueParameter, propertyChain, out var unionReadExpression, out var unionSkipExpression))
                                {
                                    unionCaseExpression =
                                        Expression.Assign(
                                            unionTypeValue,
                                            Expression.Convert(
                                                unionReadExpression,
                                                type
                                            )
                                        );
                                }

                                unionSwitchCases.Add(
                                    Expression.SwitchCase(
                                        unionCaseExpression,
                                        Expression.Constant(
                                            (long)i,
                                            typeof(long)
                                        )
                                    )
                                );
                            }

                            readExpression =
                                Expression.Block(
                                    type,
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
                                        unionSwitchCases
                                    ),
                                    unionTypeValue
                                );

                            skipExpression =
                                Expression.Call(
                                    streamParameter,
                                    typeof(IDecoder).GetMethod(nameof(IDecoder.SkipInt))
                                );
                            break;

                        default:
                            ResolveReader(origin, localReaderType, localReaderSchema, writerSchema, streamParameter, valueParameter, propertyChain, out readExpression, out skipExpression);
                            break;
                    }
                    break;

                case UnionSchema r when type.Equals(typeof(object)) && !(writerSchema is UnionSchema):
                    var nonUnionToUnionIndex = FindMatch(writerSchema, r, out var nonUnionToUnionMatch);
                    if (nonUnionToUnionIndex >= 0)
                    {
                        if(ResolveReader(origin, SchemaToType.Get(nonUnionToUnionMatch, origin), nonUnionToUnionMatch, writerSchema, streamParameter, valueParameter, propertyChain, out readExpression, out skipExpression))
                            readExpression =
                                Expression.Convert(
                                    readExpression,
                                    typeof(object)
                                );
                    }
                    break;

                case UnionSchema r when type.Equals(typeof(object)) && writerSchema is UnionSchema && (writerSchema as UnionSchema).Count > 0:
                    var unionToUnionWriterSchema = writerSchema as UnionSchema;
                    var unionToUnionReadCases = new SwitchCase[unionToUnionWriterSchema.Count];
                    var unionToUnionSkipCases = new SwitchCase[unionToUnionWriterSchema.Count];
                    var unionToUnionTypeIndex =
                        Expression.Variable(
                            typeof(long),
                            "unionTypeIndex"
                        );
                    var unionToUnionTypeValue =
                        Expression.Variable(
                            typeof(object),
                            "unionTypeValue"
                        );
                    for (int i = 0; i < unionToUnionWriterSchema.Count; i++)
                    {
                        var unionToUnionIndex = FindMatch(unionToUnionWriterSchema[i], r, out var unionToUnionMatch);
                        if (unionToUnionIndex >= 0)
                        {
                            if (ResolveReader(origin, SchemaToType.Get(unionToUnionMatch, origin), unionToUnionMatch, unionToUnionWriterSchema[i], streamParameter, valueParameter, propertyChain, out var unionToUnionRead, out var unionToUnionSkip))
                            {
                                unionToUnionReadCases[i] =
                                    Expression.SwitchCase(
                                        Expression.Assign(
                                            unionToUnionTypeValue,
                                            Expression.Convert(
                                                unionToUnionRead,
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
                                ResolveReader(origin, SchemaToType.Get(unionToUnionWriterSchema[i], origin), unionToUnionWriterSchema[i], unionToUnionWriterSchema[i], streamParameter, valueParameter, propertyChain, out var _, out unionToUnionSkip);

                                unionToUnionReadCases[i] =
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
                            }

                            unionToUnionSkipCases[i] =
                                Expression.SwitchCase(
                                    unionToUnionSkip,
                                    Expression.Constant(
                                        (long)i,
                                        typeof(long)
                                    )
                                );
                        }
                    }

                    readExpression =
                        Expression.Block(
                            type,
                            new List<ParameterExpression>()
                            {
                                unionToUnionTypeIndex,
                                unionToUnionTypeValue
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
                                unionToUnionReadCases
                            ),
                            unionToUnionTypeValue
                        );

                    skipExpression =
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
                                unionToUnionSkipCases
                            )
                        );
                    break;
                case Schema r when writerSchema is UnionSchema && (writerSchema as UnionSchema).Count > 0:
                    var unionToNonUnionWriterSchema = writerSchema as UnionSchema;
                    var unionToNonUnionReadCases = new SwitchCase[unionToNonUnionWriterSchema.Count];
                    var unionToNonUnionSkipCases = new SwitchCase[unionToNonUnionWriterSchema.Count];
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
                    for (int i = 0; i < unionToNonUnionWriterSchema.Count; i++)
                    {
                        if (ResolveReader(origin, type, r, unionToNonUnionWriterSchema[i], streamParameter, valueParameter, propertyChain, out var unionToNonUnionRead, out var unionToNonUnionSkip))
                        {
                            unionToNonUnionReadCases[i] =
                                Expression.SwitchCase(
                                    Expression.Assign(
                                        unionToNonUnionTypeValue,
                                        Expression.Convert(
                                            unionToNonUnionRead,
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
                            ResolveReader(origin, SchemaToType.Get(unionToNonUnionWriterSchema[i], origin), unionToNonUnionWriterSchema[i], unionToNonUnionWriterSchema[i], streamParameter, valueParameter, propertyChain, out var _, out unionToNonUnionSkip);

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
                        }

                        unionToNonUnionSkipCases[i] =
                            Expression.SwitchCase(
                                unionToNonUnionSkip,
                                Expression.Constant(
                                    (long)i,
                                    typeof(long)
                                )
                            );
                    }

                    readExpression =
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
                        );

                    skipExpression =
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
                        );
                    break;
            }

            if (readExpression != null && assign)
            {
                var valueExpression = GetValueExpression(valueParameter, propertyChain);
                readExpression =
                    Expression.Assign(
                        valueExpression,
                        readExpression
                    );
            }

            return readExpression != null;
        }

        private static int FindMatch(Schema schema, UnionSchema unionSchema, out Schema matchingSchema)
        {
            switch (schema)
            {
                case IntSchema s:
                    matchingSchema =
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(IntSchema))) ??
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(LongSchema))) ??
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(FloatSchema))) ??
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(DoubleSchema)))
                    ;
                    break;
                case LongSchema s:
                    matchingSchema =
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(LongSchema))) ??
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(FloatSchema))) ??
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(DoubleSchema)))
                    ;
                    break;
                case FloatSchema s:
                    matchingSchema =
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(DoubleSchema))) ??
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(FloatSchema)))
                    ;
                    break;
                case StringSchema s:
                    matchingSchema =
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(StringSchema))) ??
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(BytesSchema)))
                    ;
                    break;
                case BytesSchema s:
                    matchingSchema =
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(BytesSchema))) ??
                        unionSchema.FirstOrDefault(r => r.GetType().Equals(typeof(StringSchema)))
                    ;
                    break;
                default:
                    matchingSchema = unionSchema.FirstOrDefault(r => r.Equals(schema));
                    break;
            }

            if (matchingSchema != null)
                return unionSchema.IndexOf(matchingSchema);
            return -1;
        }
    }
}
