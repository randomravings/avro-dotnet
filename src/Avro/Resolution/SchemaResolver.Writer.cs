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
        public static Action<IAvroEncoder, T> ResolveWriter<T>(AvroSchema writerSchema) => ResolveWriter<T>(writerSchema, writerSchema);

        public static Action<IAvroEncoder, T> ResolveWriter<T>(AvroSchema sourceSchema, AvroSchema targetSchema)
        {
            var type = typeof(T);
            var assemblies = new[]
{
                type.Assembly,
                Assembly.GetCallingAssembly(),
                Assembly.GetExecutingAssembly(),
                Assembly.GetEntryAssembly(),
            }
            .GroupBy(r => r.FullName)
            .Select(g => g.First())
            .ToArray();

            var stream = Expression.Parameter(typeof(IAvroEncoder));
            var value = Expression.Parameter(type);
            if (typeof(AvroUnion).IsAssignableFrom(type) && !(targetSchema is UnionSchema))
            {
                value = Expression.Parameter(
                    type.GetMethods()
                    .Where(r => r.Name == "op_Implicit" && r.ReturnType.Equals(GetTypeFromSchema(targetSchema, assemblies)))
                    .Select(r => r.ReturnType)
                    .FirstOrDefault()
                );
            }

            var writeExpression = ResolveEncoder(sourceSchema, targetSchema, assemblies, stream, value);
            if (writeExpression == null)
                throw new AvroException($"Unable to resolve writer: '{sourceSchema}' for type: '{type}'");

            var outerValue = value;

            if (typeof(AvroUnion).IsAssignableFrom(type) && !(targetSchema is UnionSchema))
            {
                outerValue = Expression.Parameter(type);
                writeExpression =
                    Expression.Invoke(
                        Expression.Lambda(
                            writeExpression,
                            stream,
                            value
                        ),
                        stream,
                        Expression.Convert(
                            outerValue,
                            value.Type
                        )
                    );
            }

            return Expression.Lambda<Action<IAvroEncoder, T>>(
                writeExpression.Reduce(),
                stream,
                outerValue
            ).Compile();
        }

        private static Expression? ResolveEncoder(AvroSchema sourceSchema, AvroSchema targetSchema, Assembly[] assemblies, ParameterExpression stream, ParameterExpression value) =>
            (sourceSchema, targetSchema) switch
            {
                (NullSchema _, NullSchema _) => CreateNullEncoder(stream, value),
                (BooleanSchema _, BooleanSchema _) => CreateBooleanEncoder(stream, value),
                (IntSchema _, IntSchema _) => CreateIntEncoder(stream, value),
                (IntSchema _, LongSchema _) => CreateLongEncoder(stream, value),
                (IntSchema _, FloatSchema _) => CreateFloatEncoder(stream, value),
                (IntSchema _, DoubleSchema _) => CreateDoubleEncoder(stream, value),
                (LongSchema _, LongSchema _) => CreateLongEncoder(stream, value),
                (LongSchema _, FloatSchema _) => CreateFloatEncoder(stream, value),
                (LongSchema _, DoubleSchema _) => CreateDoubleEncoder(stream, value),
                (FloatSchema _, FloatSchema _) => CreateFloatEncoder(stream, value),
                (FloatSchema _, DoubleSchema _) => CreateDoubleEncoder(stream, value),
                (DoubleSchema _, DoubleSchema _) => CreateDoubleEncoder(stream, value),
                (BytesSchema _, BytesSchema _) => CreateBytesEncoder(stream, value),
                (BytesSchema _, StringSchema _) => CreateBytesEncoder(stream, value),
                (StringSchema _, StringSchema _) => CreateStringEncoder(stream, value),
                (StringSchema _, BytesSchema _) => CreateStringEncoder(stream, value),
                (UuidSchema _, UuidSchema _) => CreateUuidEncoder(stream, value),
                (DateSchema _, DateSchema _) => CreateDateEncoder(stream, value),
                (TimeMillisSchema _, TimeMillisSchema _) => CreateTimeMsEncoder(stream, value),
                (TimeMillisSchema _, TimeMicrosSchema _) => CreateTimeUsEncoder(stream, value),
                (TimeMillisSchema _, TimeNanosSchema _) => CreateTimeNsEncoder(stream, value),
                (TimeMicrosSchema _, TimeMicrosSchema _) => CreateTimeUsEncoder(stream, value),
                (TimeMicrosSchema _, TimeNanosSchema _) => CreateTimeNsEncoder(stream, value),
                (TimeNanosSchema _, TimeNanosSchema _) => CreateTimeNsEncoder(stream, value),
                (TimestampMillisSchema _, TimestampMillisSchema _) => CreateTimestampMsEncoder(stream, value),
                (TimestampMillisSchema _, TimestampMicrosSchema _) => CreateTimestampUsEncoder(stream, value),
                (TimestampMillisSchema _, TimestampNanosSchema _) => CreateTimestampNsEncoder(stream, value),
                (TimestampMicrosSchema _, TimestampMicrosSchema _) => CreateTimestampUsEncoder(stream, value),
                (TimestampMicrosSchema _, TimestampNanosSchema _) => CreateTimestampNsEncoder(stream, value),
                (TimestampNanosSchema _, TimestampNanosSchema _) => CreateTimestampNsEncoder(stream, value),
                (DurationSchema _, DurationSchema _) => CreateDurationEncoder(stream, value),
                (DecimalSchema s, DecimalSchema t) => CreateDecimalEncoder(stream, value, s, t),
                (ArraySchema s, ArraySchema t) => CreateArrayEncoder(stream, value, assemblies, s, t),
                (MapSchema s, MapSchema t) => CreateMapEncoder(stream, value, assemblies, s, t),
                (EnumSchema s, EnumSchema t) => CreateEnumEncoder(stream, value, s, t),
                (FixedSchema s, FixedSchema t) => CreateFixedEncoder(stream, value, s, t),
                (ErrorSchema s, ErrorSchema t) => CreateErrorEncoder(stream, value, assemblies, s, t),
                (RecordSchema s, RecordSchema t) => CreateRecordEncoder(stream, value, assemblies, s, t),
                (UnionSchema s, AvroSchema t) => CreateUnionToAnyEncoder(stream, value, assemblies, s, t),
                //(UnionSchema s, AvroSchema t) => CreateUnionToSingleEncoder(stream, value, assemblies, s, t),
                (AvroSchema s, UnionSchema t) => CreateSingleToUnionEncoder(stream, value, assemblies, s, t),
                (_, _) => null,
            };

        private static Expression CreateNullEncoder(ParameterExpression streamParameter, ParameterExpression value) =>
            value.Type switch
            {
                var t when typeof(AvroNull).Equals(t) =>
                    Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteNull)),
                        value
                    ),
                var t when typeof(AvroUnion).IsAssignableFrom(t) && t.GetGenericArguments().Any(r => r.GetType().Equals(typeof(AvroNull))) =>
                    Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteNull)),
                        Expression.Constant(
                            AvroNull.Value
                        )
                    ),
                var t when Nullable.GetUnderlyingType(value.Type) != null || t.IsClass || t.IsInterface =>
                    Expression.Call(
                        streamParameter,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteNull)),
                        Expression.Constant(
                            AvroNull.Value
                        )
                    ),
                var t => throw new ArgumentException($"Unsupported null type: '{t.Name}'")
            };

        private static Expression CreateBooleanEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteBoolean)),
                CastOrExpression(value, typeof(bool))
            );

        private static Expression CreateIntEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteInt)),
                CastOrExpression(value, typeof(int))
            );

        private static Expression CreateLongEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteLong)),
                CastOrExpression(value, typeof(long))
            );

        private static Expression CreateFloatEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteFloat)),
                CastOrExpression(value, typeof(float))
            );

        private static Expression CreateDoubleEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDouble)),
                CastOrExpression(value, typeof(double))
            );

        private static Expression CreateBytesEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteBytes)),
                CastOrExpression(value, typeof(byte[]))
            );

        private static Expression CreateStringEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteString)),
                CastOrExpression(value, typeof(string))
            );

        private static Expression CreateUuidEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteUuid)),
                CastOrExpression(value, typeof(Guid))
            );

        private static Expression CreateDateEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDate)),
                CastOrExpression(value, typeof(DateTime))
            );

        private static Expression CreateTimeMsEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimeMS)),
                CastOrExpression(value, typeof(TimeSpan))
            );

        private static Expression CreateTimeUsEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimeUS)),
                CastOrExpression(value, typeof(TimeSpan))
            );

        private static Expression CreateTimeNsEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimeNS)),
                CastOrExpression(value, typeof(TimeSpan))
            );

        private static Expression CreateTimestampMsEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimestampMS)),
                CastOrExpression(value, typeof(DateTime))
            );

        private static Expression CreateTimestampUsEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimestampUS)),
                CastOrExpression(value, typeof(DateTime))
            );

        private static Expression CreateTimestampNsEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteTimestampNS)),
                CastOrExpression(value, typeof(DateTime))
            );

        private static Expression CreateDurationEncoder(ParameterExpression stream, ParameterExpression value) =>
            Expression.Call(
                stream,
                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDuration)),
                CastOrExpression(value, typeof(AvroDuration))
            );

        private static Expression? CreateDecimalEncoder(ParameterExpression stream, ParameterExpression value, DecimalSchema sourceSchema, DecimalSchema targetSchema) =>
            (sourceSchema.Equals(targetSchema), targetSchema.Type) switch
            {
                (true, BytesSchema s) =>
                    Expression.Call(
                        stream,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDecimal), new Type[] { typeof(decimal), typeof(int) }),
                        CastOrExpression(value, typeof(decimal)),
                        Expression.Constant(sourceSchema.Scale, typeof(int))
                    ),
                (true, FixedSchema s) =>
                    Expression.Call(
                    stream,
                    typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteDecimal), new Type[] { typeof(decimal), typeof(int), typeof(int) }),
                    CastOrExpression(value, typeof(decimal)),
                    Expression.Constant(sourceSchema.Scale, typeof(int)),
                    Expression.Constant(s.Size, typeof(int))
                ),
                _ => default
            };

        private static Expression? CreateArrayEncoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, ArraySchema sourceSchema, ArraySchema targetSchema) =>
            GetListInterfaceDefinition(value.Type) switch
            {
                (bool isInterface, Type d, Type t) => Expression.Parameter(t) switch
                {
                    ParameterExpression p =>
                        ResolveEncoder(sourceSchema.Items, targetSchema.Items, assemblies, stream, p) switch
                        {
                            Expression write =>
                                Expression.Call(
                                    stream,
                                    isInterface switch
                                    {
                                        true =>
                                            typeof(IAvroEncoder).GetMethods()
                                            .First(r =>
                                                r.Name == nameof(IAvroEncoder.WriteArray) &&
                                                r.ContainsGenericParameters &&
                                                r.GetGenericArguments().Count() == 1
                                            )
                                            .MakeGenericMethod(t),
                                        false =>
                                            typeof(IAvroEncoder).GetMethods()
                                            .First(r =>
                                                r.Name == nameof(IAvroEncoder.WriteArray) &&
                                                r.ContainsGenericParameters &&
                                                r.GetGenericArguments().Count() == 2
                                            )
                                            .MakeGenericMethod(value.Type, t)
                                    },
                                    value,
                                    Expression.Lambda(
                                        write,
                                        stream,
                                        p
                                    )
                                ),
                            _ => default
                        },
                    _ => default
                }
            };

        private static Expression? CreateMapEncoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, MapSchema sourceSchema, MapSchema targetSchema) =>
            GetDictionaryInterfaceDefinition(value.Type) switch
            { 
                (bool isInterface, Type d, Type t) => Expression.Parameter(t) switch
                {
                    ParameterExpression p =>
                        ResolveEncoder(sourceSchema.Values, targetSchema.Values, assemblies, stream, p) switch
                        {
                            Expression write =>
                                Expression.Call(
                                    stream,
                                    isInterface switch
                                    {
                                        true =>
                                            typeof(IAvroEncoder).GetMethods()
                                            .First(r =>
                                                r.Name == nameof(IAvroEncoder.WriteMap) &&
                                                r.ContainsGenericParameters &&
                                                r.GetGenericArguments().Count() == 1
                                            )
                                            .MakeGenericMethod(t),
                                        false =>
                                            typeof(IAvroEncoder).GetMethods()
                                            .First(r =>
                                                r.Name == nameof(IAvroEncoder.WriteMap) &&
                                                r.ContainsGenericParameters &&
                                                r.GetGenericArguments().Count() == 2
                                            )
                                            .MakeGenericMethod(value.Type, t)
                                    },
                                    value,
                                    Expression.Lambda(
                                        write,
                                        stream,
                                        p
                                    )
                                ),
                            _ => default
                        },
                    _ => default
                }
            };

        private static Expression CreateEnumEncoder(ParameterExpression stream, ParameterExpression value, EnumSchema sourceSchema, EnumSchema targetSchema)
        {
            if(sourceSchema.SequenceEqual(targetSchema))
            {
                if (typeof(IAvroEnum).IsAssignableFrom(value.Type))
                    return Expression.Call(
                        stream,
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteEnum), new[] { typeof(IAvroEnum) }),
                        value
                    );
                else
                     return Expression.Call(
                         stream,
                         typeof(IAvroEncoder)
                         .GetMethods()
                         .First(
                             r => r.Name == nameof(IAvroEncoder.WriteEnum) &&
                                  r.GetGenericArguments().Length == 1 &&
                                  r.GetGenericArguments()[0]
                                    .GetGenericParameterConstraints()
                                    .SequenceEqual(new[] { typeof(Enum), typeof(ValueType) })
                         ).MakeGenericMethod(value.Type),
                         value
                     );
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private static Expression? CreateFixedEncoder(ParameterExpression stream, ParameterExpression value, FixedSchema sourceSchema, FixedSchema targetSchema) =>
            sourceSchema.Equals(targetSchema) switch
            {
                true =>
                    value.Type switch
                    {
                        var t when typeof(IAvroFixed).IsAssignableFrom(t) =>
                            Expression.Call(
                                stream,
                                typeof(IAvroEncoder)
                                .GetMethods()
                                .First(
                                    r => r.Name == nameof(IAvroEncoder.WriteFixed) &&
                                         r.GetGenericArguments().Length == 1 &&
                                         r.GetGenericArguments()[0]
                                            .GetGenericParameterConstraints()
                                            .Any(c => c.Equals(typeof(IAvroFixed)))
                                ).MakeGenericMethod(value.Type),
                                value
                            ),
                        var t when t.Equals(typeof(byte[])) =>
                            Expression.Call(
                                stream,
                                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteFixed), new[] { typeof(byte[]) }),
                                value
                            ),
                        _ => default
                    },
                _ => default
            };

        private static Expression? CreateErrorEncoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, ErrorSchema sourceSchema, ErrorSchema targetSchema)
        {
            var recordVariable = Expression.Variable(
                value.Type
            );
            var fieldVariables = new List<ParameterExpression>
            {
                recordVariable
            };
            var fieldExpressions = new List<Expression>
            {
                Expression.Assign(
                    recordVariable,
                    value
                )
            };
            int x = 0;
            foreach (var field in sourceSchema)
            {
                var fieldValueExpression = default(Expression);
                var fieldWriteExpression = default(Expression);
                var fieldType = GetTypeFromSchema(field.Type, assemblies);
                var fieldParameter = Expression.Parameter(fieldType);
                fieldVariables.Add(fieldParameter);
                if (typeof(GenericError).IsAssignableFrom(value.Type))
                {
                    var recordProperty = typeof(IAvroError).GetProperty("Item", typeof(object), new Type[] { typeof(int) });
                    fieldValueExpression =
                        Expression.Convert(
                            Expression.MakeIndex(
                                Expression.TypeAs(
                                    value,
                                    typeof(IAvroError)
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
                    fieldWriteExpression = ResolveEncoder(field.Type, field.Type, assemblies, stream, fieldParameter);
                }
                else
                {
                    var recordProperty = value.Type.GetProperty(field.Name);
                    fieldType = recordProperty.PropertyType;
                    fieldValueExpression =
                        Expression.MakeMemberAccess(
                            recordVariable,
                            recordProperty
                        );
                    fieldWriteExpression = ResolveEncoder(field.Type, field.Type, assemblies, stream, fieldParameter);
                }

                if (fieldWriteExpression == null)
                    return default;
                fieldExpressions.Add(Expression.Assign(fieldParameter, fieldValueExpression));
                fieldExpressions.Add(fieldWriteExpression);
                x++;
            }
            return Expression.Block(
                typeof(void),
                fieldVariables.ToArray(),
                fieldExpressions
            );
        }

        private static Expression? CreateRecordEncoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, RecordSchema sourceSchema, RecordSchema targetSchema)
        {
            var recordVariable = Expression.Variable(
                value.Type
            );
            var fieldVariables = new List<ParameterExpression>
            {
                recordVariable
            };
            var fieldExpressions = new List<Expression>
            {
                Expression.Assign(
                    recordVariable,
                    value
                )
            };
            int x = 0;
            foreach (var field in sourceSchema)
            {
                var fieldValueExpression = default(Expression);
                var fieldWriteExpression = default(Expression);
                var fieldType = GetTypeFromSchema(field.Type, assemblies);
                var fieldParameter = Expression.Parameter(fieldType);
                fieldVariables.Add(fieldParameter);
                if (typeof(GenericRecord).IsAssignableFrom(value.Type))
                {
                    var recordProperty = typeof(IAvroRecord).GetProperty("Item", typeof(object), new Type[] { typeof(int) });
                    fieldValueExpression =
                        Expression.Convert(
                            Expression.MakeIndex(
                                Expression.TypeAs(
                                    value,
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
                    fieldWriteExpression = ResolveEncoder(field.Type, field.Type, assemblies, stream, fieldParameter);
                }
                else
                {
                    var recordProperty = value.Type.GetProperty(field.Name);
                    fieldType = recordProperty.PropertyType;
                    fieldValueExpression =
                        Expression.MakeMemberAccess(
                            recordVariable,
                            recordProperty
                        );
                    fieldWriteExpression = ResolveEncoder(field.Type, field.Type, assemblies, stream, fieldParameter);
                }

                if (fieldWriteExpression == null)
                    return default;
                fieldExpressions.Add(Expression.Assign(fieldParameter, fieldValueExpression));
                fieldExpressions.Add(fieldWriteExpression);
                x++;
            }
            return Expression.Block(
                typeof(void),
                fieldVariables.ToArray(),
                fieldExpressions
            );
        }

        public static Expression? CreateUnionToAnyEncoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema sourceSchema, AvroSchema targetSchema) =>
            (sourceSchema, targetSchema, typeof(AvroUnion).IsAssignableFrom(value.Type)) switch
            {
                (var s, var t, false) when s.Count == 2 && s.NullIndex > -1 - 1 => CreateNullableEncoder(stream, value, assemblies, s, t),
                (var t, var s, _) => GetUnionEncoder(stream, value, assemblies, t, s)
            };

        private static Expression? GetUnionEncoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema sourceSchema, AvroSchema targetSchema) =>
            targetSchema switch
            {
                UnionSchema s => sourceSchema.Select((r, i) => GetUnionEncodeSwitchCase(stream, value, assemblies, r, s, i)).Where(r => r != default) switch
                {
                    var c when c.Count() > 0 =>
                        Expression.Switch(
                            Expression.MakeMemberAccess(
                                value,
                                value.Type.GetProperty(nameof(AvroUnion.Index))
                            ),
                            Expression.Throw(
                                Expression.Constant(new IndexOutOfRangeException())
                            ),
                            c.ToArray()
                        ),
                    _ => throw new Exception()
                },
                var s => FindMatch(s, sourceSchema) switch
                {
                    (var i, var t) when i > -1 => ResolveEncoder(t, s, assemblies, stream, Expression.Parameter(GetTypeFromSchema(s, assemblies))) switch
                    {
                        Expression write =>
                            Expression.New(
                                value.Type.GetConstructor(new[] { value.Type.GetGenericArguments()[i] }),
                                write
                            ),
                        _ => throw new Exception()
                    },
                    _ => throw new Exception(),
                }
            };

        private static SwitchCase? GetUnionEncodeSwitchCase(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, AvroSchema sourceSchema, UnionSchema targetSchema, long index) =>
            FindMatch(sourceSchema, targetSchema) switch
            {
                (int i, AvroSchema s) => Expression.Parameter(GetTypeFromSchema(s)) switch
                {
                    var v => ResolveEncoder(sourceSchema, s, assemblies, stream, v) switch
                    {
                        var write =>
                            Expression.SwitchCase(
                                Expression.Block(
                                    typeof(void),
                                    new[] { v },
                                    Expression.Assign(
                                        v,
                                        Expression.Convert(
                                            value,
                                            v.Type
                                        )
                                    ),
                                    Expression.Call(
                                        stream,
                                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteLong)),
                                        Expression.Constant((long)i)
                                    ),
                                    write
                                ),
                                Expression.Constant(index)
                            )
                    }
                },
                _ => default
            };

        private static Expression CreateNullableEncoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema sourceSchema, AvroSchema targetSchema) =>
            (targetSchema, Nullable.GetUnderlyingType(value.Type)) switch
            {
                (NullSchema _, _) =>
                    Expression.IfThenElse(
                        Expression.Equal(value, Expression.Constant(null, value.Type)),
                        CreateNullEncoder(stream, value),
                        Expression.Throw(Expression.Constant(new IndexOutOfRangeException()))
                    ),
                (UnionSchema s, var u) when s.Count == 2 && s.NullIndex > -1 => ResolveEncoder(sourceSchema[(sourceSchema.NullIndex + 1) % 2], s[(s.NullIndex + 1) % 2], assemblies, stream, value) switch
                {
                    var write => Expression.Call(
                        stream,
                        u == null ?
                            typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteNullableObject)).MakeGenericMethod(value.Type) :
                            typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteNullableValue)).MakeGenericMethod(u),
                        value,
                        Expression.Lambda(
                            typeof(Action<,>).MakeGenericType(typeof(IAvroEncoder), u ?? value.Type),
                            write,
                            stream,
                            Expression.Parameter(u ?? value.Type)
                        ),
                        Expression.Constant(
                            (long)s.NullIndex,
                            typeof(long)
                        )
                    )
                },
                (AvroSchema s, _) =>
                    Expression.IfThenElse(
                        Expression.Equal(value, Expression.Constant(null, value.Type)),
                        Expression.Throw(Expression.Constant(new IndexOutOfRangeException())),
                        ResolveEncoder(s, targetSchema, assemblies, stream, value)
                    )
            };

        public static Expression? CreateUnionToSingleEncoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema sourceSchemas, AvroSchema targetSchema)
        {
            var isValueType = Nullable.GetUnderlyingType(value.Type) != null;
            var valueType = Nullable.GetUnderlyingType(value.Type) ?? value.Type;
            var write = default(Expression);
            var nullExpression = default(Expression);
            var valueExpression = default(Expression);
            var testCondition = default(Expression);

            switch (targetSchema)
            {
                case UnionSchema r when r.Count == 2 && r.NullIndex > -1:
                    var valueSchema = r[(r.NullIndex + 1) % 2];
                    var valueWriter = isValueType ?
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteNullableValue)) :
                        typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteNullableObject))
                    ;
                    valueWriter = valueWriter.MakeGenericMethod(valueType);

                    write = ResolveEncoder(sourceSchemas, r[(r.NullIndex + 1) % 2], assemblies, stream, value);
                    if (write == null)
                        return default;

                    return Expression.Call(
                        stream,
                        valueWriter,
                        value,
                        Expression.Lambda(
                            typeof(Action<,>).MakeGenericType(typeof(IAvroEncoder), valueType),
                            write,
                            stream,
                            Expression.Parameter(valueType)
                        ),
                        Expression.Constant(
                            (long)r.NullIndex,
                            typeof(long)
                        )
                    );

                case UnionSchema r:
                    (var valueIndex, var matchingSchema) = FindMatch(sourceSchemas, r);

                    if (isValueType)
                        testCondition = Expression.MakeMemberAccess(
                            value,
                            value.Type.GetProperty(nameof(Nullable<bool>.HasValue))
                        );
                    else
                        testCondition = Expression.Equal(
                            value,
                            Expression.Constant(
                                null,
                                value.Type
                            )
                        );

                    if (r.NullIndex > -1)
                        nullExpression = Expression.Block(
                            Expression.Call(
                                stream,
                                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteLong)),
                                Expression.Constant(
                                    (long)r.NullIndex,
                                    typeof(long)
                                )
                            ),
                            CreateNullEncoder(stream, value)
                        );
                    else
                        nullExpression = Expression.Throw(
                            Expression.Constant(new ArgumentException())
                        );

                    if (valueIndex > -1)
                        valueExpression = ResolveEncoder(sourceSchemas, matchingSchema, assemblies, stream, value);

                    if (valueExpression != null)
                        valueExpression = Expression.Block(
                            Expression.Call(
                                stream,
                                typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteLong)),
                                Expression.Constant(
                                    (long)valueIndex,
                                    typeof(long)
                                )
                            ),
                            valueExpression
                        );
                    else
                        valueExpression = Expression.Throw(
                            Expression.Constant(new ArgumentException())
                        );

                    return Expression.IfThenElse(
                        testCondition,
                        nullExpression,
                        valueExpression
                    );

                case NullSchema r:
                    if (isValueType)
                        testCondition = Expression.MakeMemberAccess(
                            value,
                            value.Type.GetProperty(nameof(Nullable<bool>.HasValue))
                        );
                    else
                        testCondition = Expression.Equal(
                            value,
                            Expression.Constant(
                                null,
                                value.Type
                            )
                        );
                    nullExpression = CreateNullEncoder(stream, value);
                    return Expression.IfThenElse(
                        testCondition,
                        nullExpression,
                        Expression.Throw(
                            Expression.Constant(new ArgumentException())
                        )
                    );

                case AvroSchema r:
                    if (isValueType)
                        testCondition = Expression.MakeMemberAccess(
                            value,
                            value.Type.GetProperty(nameof(Nullable<bool>.HasValue))
                        );
                    else
                        testCondition = Expression.Equal(
                            value,
                            Expression.Constant(
                                null,
                                value.Type
                            )
                        );
                    valueExpression = ResolveEncoder(sourceSchemas, r, assemblies, stream, value);
                    if (valueExpression == null)
                        return default;

                    return Expression.IfThenElse(
                        testCondition,
                        Expression.Throw(
                            Expression.Constant(new ArgumentException())
                        ),
                        valueExpression
                    );

                default:
                    return default;
            }
        }

        public static Expression? CreateSingleToUnionEncoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, AvroSchema valueSchema, UnionSchema targetSchemas)
        {
            (var index, var matchingSchema) = FindMatch(valueSchema, targetSchemas.ToArray());
            if (index == -1)
                return default;
            var write = ResolveEncoder(valueSchema, matchingSchema, assemblies, stream, value);
            if (write == null)
                return default;
            return Expression.Block(
                Expression.Call(
                    stream,
                    typeof(IAvroEncoder).GetMethod(nameof(IAvroEncoder.WriteLong)),
                    Expression.Constant(
                        (long)index,
                        typeof(long)
                    )
                ),
                write
            );
        }
    }
}
