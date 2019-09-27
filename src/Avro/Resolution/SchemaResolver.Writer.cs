using Avro.IO;
using Avro.Schema;
using Avro.Serialization;
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
        public static IEnumerable<SerializationType> GetSerializationTypes(AvroSchema avroSchema)
        {
            var attributes = avroSchema.GetType().GetCustomAttributes<SerializationType>();
            switch (avroSchema)
            {
                case LogicalSchema l:
                    attributes = attributes.Union(GetSerializationTypes(l.Type));
                    break;
                case UnionSchema u:
                    foreach(var s in u)
                        attributes = attributes.Union(GetSerializationTypes(s));
                    break;
            }
            return attributes;
        }

        public static Action<IAvroEncoder, T> ResolveWriter<T>(AvroSchema writerSchema)
        {
            var type = typeof(T);
            var assembly = type.Assembly;
            var attributes = GetSerializationTypes(writerSchema);

            var streamParameter = Expression.Parameter(typeof(IAvroEncoder), "s");
            var valueParameter = Expression.Parameter(type, "v");
            if (type.Equals(typeof(GenericRecord)) || type.Equals(typeof(IAvroRecord)) || type.Equals(typeof(object)))
                assembly = null;
            var writeExpression = ResolveWriter(assembly, type, writerSchema, streamParameter, valueParameter, null);
            if (writeExpression == null)
                throw new AvroException($"Unable to resolve writer: '{writerSchema}' for type: '{type}'");
            return Expression.Lambda<Action<IAvroEncoder, T>>(
                writeExpression,
                streamParameter,
                valueParameter
            ).Compile();
        }

        private static Expression ResolveWriter(Assembly origin, Type type, AvroSchema writerSchema, ParameterExpression streamParameter, Expression valueExpression, Type valueParameterCast)
        {
            switch (writerSchema)
            {
                case NullSchema r when type.IsInterface || type.IsClass || (type.IsValueType && Nullable.GetUnderlyingType(type) != null):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteNull)),
                        Expression.Constant(
                            AvroNull.Value,
                            typeof(AvroNull)
                        )
                    );
                case BooleanSchema r when type.Equals(typeof(bool)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteBoolean)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case IntSchema r when type.Equals(typeof(int)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteInt)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case LongSchema r when type.Equals(typeof(long)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteLong)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case FloatSchema r when type.Equals(typeof(float)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteFloat)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case DoubleSchema r when type.Equals(typeof(double)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDouble)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case BytesSchema r when type.Equals(typeof(byte[])):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteBytes)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case StringSchema r when type.Equals(typeof(string)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteString)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case UuidSchema r when type.Equals(typeof(Guid)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteUuid)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case DateSchema r when type.Equals(typeof(DateTime)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDate)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case TimeMillisSchema r when type.Equals(typeof(TimeSpan)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimeMS)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case TimeMicrosSchema r when type.Equals(typeof(TimeSpan)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimeUS)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case TimeNanosSchema r when type.Equals(typeof(TimeSpan)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimeNS)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case TimestampMillisSchema r when type.Equals(typeof(DateTime)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimestampMS)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case TimestampMicrosSchema r when type.Equals(typeof(DateTime)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimestampUS)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case TimestampNanosSchema r when type.Equals(typeof(DateTime)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimestampNS)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case DurationSchema r when type.Equals(typeof(AvroDuration)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDuration)),
                        CastOrExpression(valueExpression, valueParameterCast)
                    );
                case DecimalSchema r when (r.Type is BytesSchema) && type.Equals(typeof(decimal)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDecimal), new Type[] { typeof(decimal), typeof(int) }),
                        CastOrExpression(valueExpression, valueParameterCast),
                        Expression.Constant(r.Scale, typeof(int))
                    );
                case DecimalSchema r when (r.Type is FixedSchema) && type.Equals(typeof(decimal)):
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDecimal), new Type[] { typeof(decimal), typeof(int), typeof(int) }),
                        CastOrExpression(valueExpression, valueParameterCast),
                        Expression.Constant(r.Scale, typeof(int)),
                        Expression.Constant((r.Type as FixedSchema).Size, typeof(int))
                    );
                case ArraySchema r when type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IList<>):
                    var arrayItemType = type.GenericTypeArguments.Last();
                    var arrayItemParameter = Expression.Parameter(arrayItemType, "i");
                    var arrayItemExpression = ResolveWriter(origin, arrayItemType, r.Items, streamParameter, arrayItemParameter, null);
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteArray)).MakeGenericMethod(arrayItemType),
                        CastOrExpression(valueExpression, valueParameterCast),
                        Expression.Lambda(
                            arrayItemExpression,
                            streamParameter,
                            arrayItemParameter
                        )
                    );
                case MapSchema r when type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IDictionary<,>) && type.GetGenericArguments().First() == typeof(string):
                    var mapValueType = type.GenericTypeArguments.Last();
                    var mapValueParameter = Expression.Parameter(mapValueType, "m");
                    var mapItemExpression = ResolveWriter(origin, mapValueType, r.Values, streamParameter, mapValueParameter, null);
                    return Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteMap)).MakeGenericMethod(mapValueType),
                        CastOrExpression(valueExpression, valueParameterCast),
                        Expression.Lambda(
                            mapItemExpression,
                            streamParameter,
                            mapValueParameter
                        )
                    );
                case EnumSchema r when typeof(GenericEnum).IsAssignableFrom(type) || type.Equals(typeof(object)) || (type.IsEnum && (Enum.GetNames(type).Intersect(r.Symbols).Count() == r.Symbols.Count)):
                    if (typeof(GenericEnum).IsAssignableFrom(type))
                    {
                        return Expression.Call(
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
                        return Expression.Switch(
                            CastOrExpression(valueExpression, valueParameterCast),
                            switchCases
                        );
                    }
                case FixedSchema r when typeof(IAvroFixed).IsAssignableFrom(type) || type.Equals(typeof(object)):
                    if (typeof(GenericFixed).IsAssignableFrom(type) || type.Equals(typeof(object)))
                    {
                        return Expression.Call(
                            streamParameter,
                            typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteFixed)),
                            Expression.Convert(valueExpression, typeof(byte[]))
                        );
                    }
                    else
                    {
                        return Expression.Call(
                            streamParameter,
                            typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteFixed)),
                            CastOrExpression(Expression.Convert(valueExpression, typeof(byte[])), valueParameterCast)
                        );
                    }
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
                            var fieldExpression = ResolveWriter(origin, fieldType, field.Type, streamParameter, fieldValueExpression, fieldType);
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
                            var fieldExpression = ResolveWriter(origin, fieldType, field.Type, streamParameter, fieldValueExpression, null);
                            fieldExpressions.Add(fieldExpression);
                        }
                    }
                    return Expression.Block(
                        fieldExpressions
                    );
                case UnionSchema r when (Nullable.GetUnderlyingType(type) != null || type.IsInterface || type.IsClass) && r.Count == 2 && r.FirstOrDefault(n => n.GetType().Equals(typeof(NullSchema))) != null:
                    var nullIndex = 0;
                    if (!r[nullIndex].GetType().Equals(typeof(NullSchema)))
                        nullIndex = 1;
                    var localValueExpression = valueExpression;
                    if (Nullable.GetUnderlyingType(type) != null)
                    {
                        localValueExpression =
                            Expression.MakeMemberAccess(
                                localValueExpression,
                                type.GetProperty("Value")
                            );
                    }

                    var valueType = Nullable.GetUnderlyingType(type) ?? type;
                    var nullableValueMethod =
                        valueType.IsClass || valueType.IsInterface ?
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteNullableObject)).MakeGenericMethod(valueType) :
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteNullableValue)).MakeGenericMethod(valueType)
                    ;

                    var writeNotNullExpression = ResolveWriter(origin, valueType, r[(nullIndex + 1) % 2], streamParameter, localValueExpression, null);

                    return Expression.Call(
                        streamParameter,
                        nullableValueMethod,
                        CastOrExpression(valueExpression, valueParameterCast),
                        Expression.Lambda(
                            writeNotNullExpression,
                            streamParameter,
                            Expression.Parameter(valueType, "v")
                        ),
                        Expression.Constant(
                            (long)nullIndex,
                            typeof(long)
                        )
                    );
                case UnionSchema r when type.Equals(typeof(object)) && r.Count > 0:
                    var writeExpression =
                        Expression.Throw(
                            Expression.Constant(new ArgumentException())
                        ) as Expression;
                    for (int i = 0; i < r.Count; i++)
                    {
                        var unionSubType = GetTypeFromSchema(r[i], origin);
                        var unionSubExpression = ResolveWriter(origin, unionSubType, r[i], streamParameter, valueExpression, unionSubType);
                        writeExpression =
                            Expression.IfThenElse((
                                r[i] is NullSchema ?
                                Expression.Equal(
                                    valueExpression,
                                    Expression.Constant(
                                        AvroNull.Value,
                                        typeof(AvroNull)
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
                    return writeExpression;
            }

            return null;
        }
    }
}
