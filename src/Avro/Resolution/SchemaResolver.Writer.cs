using Avro.IO;
using Avro.Schema;
using Avro.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Avro.Resolution
{
    public static partial class SchemaResolver
    {
        public static Action<IAvroEncoder, T> ResolveWriter<T>(AvroSchema writerSchema)
        {
            var type = typeof(T);
            var streamParameter = Expression.Parameter(typeof(IAvroEncoder), "s");
            var valueParameter = Expression.Parameter(type, "v");
            var writeAction = typeof(Action<,>).MakeGenericType(typeof(IAvroEncoder), type);
            var assembly = type.Assembly;
            if (type.Equals(typeof(GenericRecord)) || type.Equals(typeof(IAvroRecord)) || type.Equals(typeof(object)))
                assembly = null;
            if (!ResolveWriter(assembly, type, writerSchema, streamParameter, valueParameter, null, out var writeExpression))
                throw new AvroException($"Unable to resolve writer: '{writerSchema}' for type: '{type}'");


            return
                Expression.Lambda(
                    writeAction,
                    writeExpression,
                    streamParameter,
                    valueParameter
                )
                .Compile() as Action<IAvroEncoder, T>;
        }

        private static bool ResolveWriter(Assembly origin, Type type, AvroSchema writerSchema, ParameterExpression streamParameter, Expression valueExpression, Type valueParameterCast, out Expression writeExpression)
        {
            writeExpression = null;

            switch (writerSchema)
            {
                case NullSchema r when type.IsInterface || type.IsClass || (type.IsValueType && Nullable.GetUnderlyingType(type) != null):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteNull))
                    );
                    break;

                case BooleanSchema r when type.Equals(typeof(bool)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteBoolean)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case IntSchema r when type.Equals(typeof(int)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteInt)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case LongSchema r when type.Equals(typeof(long)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteLong)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case FloatSchema r when type.Equals(typeof(float)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteFloat)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case DoubleSchema r when type.Equals(typeof(double)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDouble)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case BytesSchema r when type.Equals(typeof(byte[])):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteBytes)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case StringSchema r when type.Equals(typeof(string)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteString)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case UuidSchema r when type.Equals(typeof(Guid)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteUuid)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case DateSchema r when type.Equals(typeof(DateTime)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDate)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case TimeMillisSchema r when type.Equals(typeof(TimeSpan)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimeMS)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case TimeMicrosSchema r when type.Equals(typeof(TimeSpan)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimeUS)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case TimeNanosSchema r when type.Equals(typeof(TimeSpan)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimeNS)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case TimestampMillisSchema r when type.Equals(typeof(DateTime)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimestampMS)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case TimestampMicrosSchema r when type.Equals(typeof(DateTime)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimestampUS)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case TimestampNanosSchema r when type.Equals(typeof(DateTime)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimestampNS)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case DurationSchema r when type.Equals(typeof(AvroDuration)):
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDuration)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                    break;

                case DecimalSchema r when type.Equals(typeof(decimal)):
                    switch (r.Type)
                    {
                        case BytesSchema t:
                            writeExpression = Expression.Call(
                                streamParameter,
                                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDecimal), new Type[] { typeof(decimal), typeof(int) }),
                                CastOrExpression(valueExpression, valueParameterCast),
                                Expression.Constant(r.Scale, typeof(int))
                            );
                            break;
                        case FixedSchema t:
                            writeExpression = Expression.Call(
                                streamParameter,
                                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDecimal), new Type[] { typeof(decimal), typeof(int), typeof(int) }),
                                CastOrExpression(valueExpression, valueParameterCast),
                                Expression.Constant(r.Scale, typeof(int)),
                                Expression.Constant(t.Size, typeof(int))
                            );
                            break;
                    }
                    break;

                case ArraySchema r when type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IList<>):
                    var arrayItemType = type.GenericTypeArguments.Last();
                    var arrayItemParameter = Expression.Parameter(arrayItemType, "i");
                    var arrayItemWriteAction = typeof(Action<,>).MakeGenericType(typeof(IAvroEncoder), arrayItemType);
                    ResolveWriter(origin, arrayItemType, r.Items, streamParameter, arrayItemParameter, null, out var arrayItemExpression);
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteArray)).MakeGenericMethod(arrayItemType),
                        CastOrExpression(valueExpression, valueParameterCast),
                        Expression.Lambda(
                            arrayItemWriteAction,
                            arrayItemExpression,
                            streamParameter,
                            arrayItemParameter
                        )
                    );
                    break;

                case MapSchema r when type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IDictionary<,>) && type.GetGenericArguments().First() == typeof(string):
                    var mapValueType = type.GenericTypeArguments.Last();
                    var mapValueParameter = Expression.Parameter(mapValueType, "m");
                    var mapValueWriteAction = typeof(Action<,>).MakeGenericType(typeof(IAvroEncoder), mapValueType);
                    ResolveWriter(origin, mapValueType, r.Values, streamParameter, mapValueParameter, null, out var mapItemExpression);
                    writeExpression = Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteMap)).MakeGenericMethod(mapValueType),
                        CastOrExpression(valueExpression, valueParameterCast),
                        Expression.Lambda(
                            mapValueWriteAction,
                            mapItemExpression,
                            streamParameter,
                            mapValueParameter
                        )
                    );
                    break;

                case EnumSchema r when typeof(GenericEnum).IsAssignableFrom(type) || type.Equals(typeof(object)) || (type.IsEnum && (Enum.GetNames(type).Intersect(r.Symbols).Count() == r.Symbols.Count)):
                    if (typeof(GenericEnum).IsAssignableFrom(type))
                    {
                        writeExpression =
                            Expression.Call(
                                streamParameter,
                                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteInt)),
                                Expression.MakeMemberAccess(
                                    valueExpression,
                                    typeof(GenericEnum).GetProperty(nameof(GenericEnum.Value))
                                )
                            );
                    }
                    else
                    {
                        var switchCases = new SwitchCase[r.Symbols.Count];
                        for (int i = 0; i < r.Symbols.Count; i++)
                        {
                            switchCases[i] =
                                Expression.SwitchCase(
                                    Expression.Call(
                                        streamParameter,
                                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteInt)),
                                        Expression.Constant(i, typeof(int))
                                    ),
                                    Expression.Constant(
                                        Enum.Parse(type, r.Symbols[i]),
                                        type
                                    )
                                );
                        }
                        writeExpression =
                            Expression.Switch(
                                CastOrExpression(valueExpression, valueParameterCast),
                                switchCases
                            );
                    }
                    break;

                case FixedSchema r when typeof(IAvroFixed).IsAssignableFrom(type) || type.Equals(typeof(object)):
                    if (typeof(GenericFixed).IsAssignableFrom(type) || type.Equals(typeof(object)))
                    {
                        writeExpression = Expression.Call(
                            streamParameter,
                            typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteFixed)),
                            Expression.Convert(valueExpression, typeof(byte[]))
                        );
                    }
                    else
                    {
                        writeExpression = Expression.Call(
                            streamParameter,
                            typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteFixed)),
                            CastOrExpression(Expression.Convert(valueExpression, typeof(byte[])), valueParameterCast)
                        );
                    }
                    break;

                case RecordSchema r when typeof(IAvroRecord).IsAssignableFrom(type) || type.Equals(typeof(object)):
                    var fieldExpressions = new List<Expression>();
                    int x = 0;
                    foreach (var field in r)
                    {
                        var fieldType = default(Type);
                        var fieldValueExpression = default(Expression);
                        if (typeof(GenericRecord).IsAssignableFrom(type) || type.Equals(typeof(IAvroRecord)) || type.Equals(typeof(object)))
                        {
                            var recordProperty = typeof(IAvroRecord).GetProperty("Item", typeof(object), new Type[] { typeof(int) });
                            fieldType = GetTypeFromSchema(field.Type, origin);
                            fieldValueExpression =
                                Expression.Convert(
                                    Expression.MakeIndex(
                                        Expression.TypeAs(
                                            valueExpression,
                                            typeof(IAvroRecord)
                                        ),
                                        recordProperty,
                                        new Expression[] {
                                            Expression.Constant(
                                                x,
                                                typeof(int)
                                            )
                                        }
                                    ),
                                    fieldType
                                );
                            ResolveWriter(origin, fieldType, field.Type, streamParameter, fieldValueExpression, fieldType, out var fieldExpression);
                            fieldExpressions.Add(fieldExpression);
                            x++;
                        }
                        else
                        {
                            var recordProperty = type.GetProperty(field.Name);
                            fieldType = recordProperty.PropertyType;
                            fieldValueExpression =
                                Expression.MakeMemberAccess(
                                    valueExpression,
                                    recordProperty
                                );
                            ResolveWriter(origin, fieldType, field.Type, streamParameter, fieldValueExpression, null, out var fieldExpression);
                            fieldExpressions.Add(fieldExpression);
                        }
                    }
                    writeExpression = Expression.Block(fieldExpressions);
                    break;

                case UnionSchema r when (Nullable.GetUnderlyingType(type) != null || type.IsInterface || type.IsClass) && r.Count == 2 && r.FirstOrDefault(n => n.GetType().Equals(typeof(NullSchema))) != null:
                    var localType = type;
                    var nullIndex = 0;
                    if (!r[nullIndex].GetType().Equals(typeof(NullSchema)))
                        nullIndex = 1;
                    var localValueExpression = valueExpression;
                    if (Nullable.GetUnderlyingType(type) != null)
                    {
                        localType = Nullable.GetUnderlyingType(type);
                        localValueExpression =
                            Expression.MakeMemberAccess(
                                localValueExpression,
                                type.GetProperty("Value")
                            );
                    }
                    ResolveWriter(origin, localType, r[(nullIndex + 1) % 2], streamParameter, localValueExpression, null, out var writeNotNullExpression);
                    writeExpression =
                        Expression.IfThenElse
                        (
                            Expression.Equal(
                                valueExpression,
                                Expression.Constant(null, type)
                            ),
                            Expression.Call(
                                streamParameter,
                                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteLong)),
                                Expression.Constant(
                                    (long)nullIndex,
                                    typeof(long)
                                )
                            ),
                            Expression.Block(
                                typeof(void),
                                Expression.Call(
                                    streamParameter,
                                    typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteLong)),
                                    Expression.Constant(
                                        (long)(nullIndex + 1) % 2,
                                        typeof(long)
                                    )
                                ),
                                writeNotNullExpression
                            )
                        );
                    break;

                case UnionSchema r when type.Equals(typeof(object)) && r.Count > 0:
                    writeExpression =
                        Expression.Throw(
                            Expression.Constant(new ArgumentException())
                        ) as Expression;
                    for (int i = 0; i < r.Count; i++)
                    {
                        var unionSubType = GetTypeFromSchema(r[i], origin);
                        ResolveWriter(origin, unionSubType, r[i], streamParameter, valueExpression, unionSubType, out var unionSubExpression);
                        writeExpression =
                            Expression.IfThenElse((
                                r[i] is NullSchema ?
                                Expression.Equal(
                                    valueExpression,
                                    Expression.Constant(
                                        null,
                                        typeof(object)
                                    )
                                ) as Expression :
                                Expression.TypeIs(
                                    valueExpression,
                                    unionSubType
                                ) as Expression),
                                Expression.Block(
                                    typeof(void),
                                    Expression.Call(
                                        streamParameter,
                                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteLong)),
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
