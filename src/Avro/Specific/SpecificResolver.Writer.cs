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

                case DurationSchema r when type.Equals(typeof(ValueTuple<int, int, int>)):
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
                                typeof(IEncoder).GetMethod(nameof(IEncoder.WriteDecimal), new Type[] { typeof(decimal), typeof(int) }),
                                CastOrExpression(valueParameterOrProperty, valueParameterCast),
                                Expression.Constant(r.Scale, typeof(int))
                            );
                            break;
                        case FixedSchema t:
                            writeExpression = Expression.Call(
                                streamParameter,
                                typeof(IEncoder).GetMethod(nameof(IEncoder.WriteDecimal), new Type[] { typeof(decimal), typeof(int), typeof(int) }),
                                CastOrExpression(valueParameterOrProperty, valueParameterCast),
                                Expression.Constant(r.Scale, typeof(int)),
                                Expression.Constant(t.Size, typeof(int))
                            );
                            break;
                    }
                    break;

                case ArraySchema r when type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IList<>):
                    var arrayItemType = type.GenericTypeArguments.Last();
                    var arrayItemParameter = Expression.Parameter(arrayItemType, "i");
                    var arrayItemWriteAction = typeof(Action<,>).MakeGenericType(typeof(IEncoder), arrayItemType);
                    ResolveWriter(origin, arrayItemType, r.Items, streamParameter, arrayItemParameter, new Stack<PropertyInfo>(), null, out var arrayItemExpression);
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteArray)).MakeGenericMethod(arrayItemType),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast),
                        Expression.Lambda(
                            arrayItemWriteAction,
                            arrayItemExpression,
                            streamParameter,
                            arrayItemParameter
                        )
                    );
                    break;

                case MapSchema r when type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IDictionary<,>):
                    var mapValueType = type.GenericTypeArguments.Last();
                    var mapValueParameter = Expression.Parameter(mapValueType, "m");
                    var mapValueWriteAction = typeof(Action<,>).MakeGenericType(typeof(IEncoder), mapValueType);
                    ResolveWriter(origin, mapValueType, r.Values, streamParameter, mapValueParameter, new Stack<PropertyInfo>(), null, out var mapItemExpression);
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IEncoder).GetMethod(nameof(IEncoder.WriteMap)).MakeGenericMethod(mapValueType),
                        CastOrExpression(valueParameterOrProperty, valueParameterCast),
                        Expression.Lambda(
                            mapValueWriteAction,
                            mapItemExpression,
                            streamParameter,
                            mapValueParameter
                        )                        
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
                        var unionSubType = GetTypeFromSchema(r[i], origin);
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
    }
}
