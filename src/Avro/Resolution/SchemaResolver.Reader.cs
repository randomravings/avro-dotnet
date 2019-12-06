using Avro.IO;
using Avro.Schema;
using Avro.Types;
using Avro.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Avro.Resolution
{
    public static partial class SchemaResolver
    {
        public static (Func<IAvroDecoder, T>, Action<IAvroDecoder>) ResolveReader<T>(AvroSchema targetSchema) => ResolveReader<T>(targetSchema, targetSchema);

        public static (Func<IAvroDecoder, T>, Action<IAvroDecoder>) ResolveReader<T>(AvroSchema targetSchema, AvroSchema sourceSchema)
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

            var stream = Expression.Parameter(typeof(IAvroDecoder));
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

            var read = ResolveDecode(assemblies, targetSchema, sourceSchema, stream, value);
            if (read == null)
                throw new AvroException($"Unable to resolve target schema: '{targetSchema}' from source schema: '{sourceSchema}'");
            var skip = ResolveSkip(sourceSchema, stream);

            read = Expression.Convert(read, type);
            return
                (
                    Expression.Lambda<Func<IAvroDecoder, T>>(
                        read.Reduce(),
                        stream
                    ).Compile(),
                    Expression.Lambda<Action<IAvroDecoder>>(
                        skip.Reduce(),
                        stream
                    ).Compile()
                 );
        }

        private static Expression? ResolveDecode(Assembly[] assemblies, AvroSchema targetSchema, AvroSchema sourceSchema, ParameterExpression stream, ParameterExpression value) =>
            (targetSchema, sourceSchema) switch
            {
                (NullSchema _, NullSchema _) => CreateNullDecode(stream, value),
                (BooleanSchema _, BooleanSchema _) => CreateBooleanDecode(stream, value),
                (IntSchema _, IntSchema _) => CreateIntDecode(stream, value),
                (LongSchema _, LongSchema _) => CreateLongDecode(stream, value),
                (LongSchema _, IntSchema _) => CreateIntDecode(stream, value),
                (FloatSchema _, FloatSchema _) => CreateFloatDecode(stream, value),
                (FloatSchema _, LongSchema _) => CreateLongDecode(stream, value),
                (FloatSchema _, IntSchema _) => CreateIntDecode(stream, value),
                (DoubleSchema _, DoubleSchema _) => CreateDoubleDecode(stream, value),
                (DoubleSchema _, FloatSchema _) => CreateFloatDecode(stream, value),
                (DoubleSchema _, LongSchema _) => CreateLongDecode(stream, value),
                (DoubleSchema _, IntSchema _) => CreateIntDecode(stream, value),
                (BytesSchema _, BytesSchema _) => CreateBytesDecode(stream, value),
                (BytesSchema _, StringSchema _) => CreateBytesDecode(stream, value),
                (StringSchema _, StringSchema _) => CreateStringDecode(stream, value),
                (StringSchema _, BytesSchema _) => CreateStringDecode(stream, value),
                (UuidSchema _, UuidSchema _) => CreateUuidDecode(stream, value),
                (DateSchema _, DateSchema _) => CreateDateDecode(stream, value),
                (TimeMillisSchema _, TimeMillisSchema _) => CreateTimeMsDecode(stream, value),
                (TimeMicrosSchema _, TimeMicrosSchema _) => CreateTimeUsDecode(stream, value),
                (TimeMicrosSchema _, TimeMillisSchema _) => CreateTimeMsDecode(stream, value),
                (TimeNanosSchema _, TimeNanosSchema _) => CreateTimeNsDecode(stream, value),
                (TimeNanosSchema _, TimeMicrosSchema _) => CreateTimeUsDecode(stream, value),
                (TimeNanosSchema _, TimeMillisSchema _) => CreateTimeMsDecode(stream, value),
                (TimestampMillisSchema _, TimestampMillisSchema _) => CreateTimestampMsDecode(stream, value),
                (TimestampMicrosSchema _, TimestampMicrosSchema _) => CreateTimestampUsDecode(stream, value),
                (TimestampMicrosSchema _, TimestampMillisSchema _) => CreateTimestampMsDecode(stream, value),
                (TimestampNanosSchema _, TimestampNanosSchema _) => CreateTimestampNsDecode(stream, value),
                (TimestampNanosSchema _, TimestampMicrosSchema _) => CreateTimestampUsDecode(stream, value),
                (TimestampNanosSchema _, TimestampMillisSchema _) => CreateTimestampMsDecode(stream, value),
                (DurationSchema _, DurationSchema _) => CreateDurationDecode(stream, value),
                (DecimalSchema t, DecimalSchema s) => CreateDecimalDecode(stream, value, t, s),
                (ArraySchema t, ArraySchema s) => CreateArrayDecode(stream, value, assemblies, t, s),
                (MapSchema t, MapSchema s) => CreateMapDecode(stream, value, assemblies, t, s),
                (EnumSchema t, EnumSchema s) => CreateEnumDecode(stream, value, t, s),
                (FixedSchema t, FixedSchema s) => CreateFixedDecode(stream, value, t, s),
                (RecordSchema t, RecordSchema s) => CreateRecordDecode(stream, value, assemblies, t, s),
                (ErrorSchema t, ErrorSchema s) => CreateErrorDecode(stream, value, assemblies, t, s),
                (UnionSchema t, AvroSchema s) => CreateUnionFromAnyDecode(stream, value, assemblies, t, s),
                (AvroSchema t, UnionSchema s) => CreateAnyFromUnionDecode(stream, value, assemblies, t, s),
                (FieldSchema t, AvroSchema s) => ResolveDecode(assemblies, t.Type, s, stream, value),
                (AvroSchema t, FieldSchema s) => ResolveDecode(assemblies, t, s.Type, stream, value),
                (LogicalSchema t, AvroSchema s) => ResolveDecode(assemblies, t.Type, s, stream, value),
                (AvroSchema t, LogicalSchema s) => ResolveDecode(assemblies, t, s.Type, stream, value),
                _ => default
            };

        private static Expression? ResolveSkip(AvroSchema sourceSchema, ParameterExpression stream) =>
            sourceSchema switch
            {
                NullSchema _ => CreateNullSkip(stream),
                BooleanSchema _ => CreateBooleanSkip(stream),
                IntSchema _ => CreateIntSkip(stream),
                LongSchema _ => CreateLongSkip(stream),
                FloatSchema _ => CreateFloatSkip(stream),
                DoubleSchema _ => CreateDoubleSkip(stream),
                BytesSchema _ => CreateBytesSkip(stream),
                StringSchema _ => CreateStringSkip(stream),
                UuidSchema _ => CreateUuidSkip(stream),
                DateSchema _ => CreateDateSkip(stream),
                TimeMillisSchema _ => CreateTimeMsSkip(stream),
                TimeMicrosSchema _ => CreateTimeUsSkip(stream),
                TimeNanosSchema _ => CreateTimeNsSkip(stream),
                TimestampMillisSchema _ => CreateTimestampMsSkip(stream),
                TimestampMicrosSchema _ => CreateTimestampUsSkip(stream),
                TimestampNanosSchema _ => CreateTimestampNsSkip(stream),
                DurationSchema _ => CreateDurationSkip(stream),
                DecimalSchema s => CreateDecimalSkip(stream, s),
                ArraySchema s => CreateArraySkip(stream, s),
                MapSchema s => CreateMapSkip(stream, s),
                EnumSchema _ => CreateEnumSkip(stream),
                FixedSchema s => CreateFixedSkip(stream, s),
                RecordSchema s => CreateRecordSkip(stream, s),
                ErrorSchema s => CreateErrorSkip(stream, s),
                UnionSchema s => CreateUnionSkip(stream, s),
                FieldSchema s => ResolveSkip(s.Type, stream),
                LogicalSchema s => ResolveSkip(s.Type, stream),
                _ => default
            };

        private static Expression CreateNullDecode(ParameterExpression stream, ParameterExpression value) =>
            value.Type switch
            {
                var t when typeof(AvroNull).Equals(t) =>
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadNull))
                    ),
                var t when typeof(AvroUnion).IsAssignableFrom(t) && t.GetGenericArguments().Any(r => r.GetType().Equals(typeof(AvroNull))) =>
                    Expression.New(
                        t.GetConstructor(new[] { typeof(AvroNull) }),
                        Expression.Call(
                            stream,
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadNull))
                        )
                    ),
                var t when Nullable.GetUnderlyingType(value.Type) != null || t.IsClass || t.IsInterface =>
                    Expression.Block(
                        value.Type,
                        Expression.Call(
                            stream,
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadNull))
                        ),
                        Expression.Default(t)
                    ),
                var t => throw new ArgumentException($"Unsupported null type: '{t.Name}'")
            }
        ;

        private static Expression CreateNullSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipNull))
            )
        ;

        private static Expression CreateBooleanDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadBoolean))
                ),
                value.Type
            )
        ;

        private static Expression CreateBooleanSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipBoolean))
            )
        ;

        private static Expression CreateIntDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadInt))
                ),
                value.Type
            )
        ;

        private static Expression CreateIntSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipInt))
            )
        ;

        private static Expression CreateLongDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadLong))
                ),
                value.Type
            )
        ;

        private static Expression CreateLongSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipLong))
            )
        ;

        private static Expression CreateFloatDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadFloat))
                ),
                value.Type
            )
        ;

        private static Expression CreateFloatSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipFloat))
            )
        ;

        private static Expression CreateDoubleDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadDouble))
                ),
                value.Type
            )
        ;

        private static Expression CreateDoubleSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipDouble))
            )
        ;

        private static Expression CreateBytesDecode(ParameterExpression stream, ParameterExpression value) =>
            value.Type switch
            {
                var t when t.Equals(typeof(string)) => 
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadString))
                    ),
                _ =>
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadBytes))
                    )
            }
        ;

        private static Expression CreateBytesSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipBytes))
            )
        ;

        private static Expression CreateStringDecode(ParameterExpression stream, ParameterExpression value) =>
            value.Type switch
            {
                var t when t.Equals(typeof(byte[])) =>
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadBytes))
                    ),
                _ =>
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadString))
                    )
            }
        ;

        private static Expression CreateStringSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipString))
            )
        ;

        private static Expression CreateUuidDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadUuid))
                ),
                value.Type
            )
        ;

        private static Expression CreateUuidSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipUuid))
            )
        ;

        private static Expression CreateDateDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadDate))
                ),
                value.Type
            )
        ;

        private static Expression CreateDateSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipDate))
            )
        ;

        private static Expression CreateTimeMsDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadTimeMS))
                ),
                value.Type
            )
        ;

        private static Expression CreateTimeMsSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipTimeMS))
            )
        ;

        private static Expression CreateTimeUsDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadTimeUS))
                ),
                value.Type
            )
        ;

        private static Expression CreateTimeUsSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipTimeUS))
            )
        ;

        private static Expression CreateTimeNsDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadTimeNS))
                ),
                value.Type
            )
        ;

        private static Expression CreateTimeNsSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipTimeNS))
            )
        ;

        private static Expression CreateTimestampMsDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadTimestampMS))
                ),
                value.Type
            )
        ;

        private static Expression CreateTimestampMsSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipTimestampMS))
            )
        ;

        private static Expression CreateTimestampUsDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadTimestampUS))
                ),
                value.Type
            )
        ;

        private static Expression CreateTimestampUsSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipTimestampUS))
            )
        ;

        private static Expression CreateTimestampNsDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadTimestampNS))
                ),
                value.Type
            )
        ;

        private static Expression CreateTimestampNsSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipTimestampNS))
            )
        ;

        private static Expression CreateDurationDecode(ParameterExpression stream, ParameterExpression value) =>
            CastOrExpression(
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadDuration))
                ),
                value.Type
            )
        ;

        private static Expression CreateDurationSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipDuration))
            )
        ;

        private static Expression CreateDecimalDecode(ParameterExpression stream, ParameterExpression value, DecimalSchema targetSchema, DecimalSchema sourceSchema) =>
            (targetSchema.Equals(sourceSchema), sourceSchema.Type) switch
            {
                (true, BytesSchema s) =>
                    CastOrExpression(
                        Expression.Call(
                            stream,
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadDecimal), new Type[] { typeof(int) }),
                            Expression.Constant(targetSchema.Scale)
                        ),
                        value.Type
                    ),
                (true, FixedSchema s) =>
                    CastOrExpression(
                        Expression.Call(
                            stream,
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadDecimal), new Type[] { typeof(int), typeof(int) }),
                            Expression.Constant(targetSchema.Scale),
                            Expression.Constant(s.Size)
                        ),
                        value.Type
                    ),
                (_, _) => throw new ArgumentException($"Decimal Type mismatch.")
            };

        private static Expression? CreateDecimalSkip(ParameterExpression stream, DecimalSchema sourceSchema) =>
            ResolveSkip(sourceSchema.Type, stream)
        ;

        private static Expression? CreateArrayDecode(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, ArraySchema targetSchema, ArraySchema sourceSchema) =>
            GetListInterfaceDefinition(value.Type) switch
            {
                (bool isInterface, Type i, Type t) => ResolveDecode(assemblies, targetSchema.Items, sourceSchema.Items, stream, Expression.Parameter(t)) switch
                {
                    Expression read =>
                        Expression.Call(
                            stream,
                            isInterface switch
                            {
                                true =>
                                    typeof(IAvroDecoder).GetMethods()
                                    .First(r =>
                                        r.Name == nameof(IAvroDecoder.ReadArray) &&
                                        r.ContainsGenericParameters &&
                                        r.GetGenericArguments().Count() == 1
                                    )
                                    .MakeGenericMethod(t),
                                false =>
                                    typeof(IAvroDecoder).GetMethods()
                                    .First(r =>
                                        r.Name == nameof(IAvroDecoder.ReadArray) &&
                                        r.ContainsGenericParameters &&
                                        r.GetGenericArguments().Count() == 2 &&
                                        r.GetParameters().Count() == 1 &&
                                        r.ReturnType.Equals(r.GetGenericArguments()[0]) &&
                                        r.GetParameters()[0].ParameterType.Equals(typeof(Func<,>).MakeGenericType(typeof(IAvroDecoder), r.GetGenericArguments()[1]))
                                    )
                                    .MakeGenericMethod(value.Type, t)
                            },
                            Expression.Lambda(
                                typeof(Func<,>).MakeGenericType(typeof(IAvroDecoder), t),
                                read,
                                stream
                            )
                        ),
                    _ => default
                }
            };

        private static Expression CreateArraySkip(ParameterExpression stream, ArraySchema sourceSchema) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipArray)),
                Expression.Lambda(
                    typeof(Action<>).MakeGenericType(typeof(IAvroDecoder)),
                    ResolveSkip(sourceSchema.Items, stream),
                    stream
                )
            )
        ;

        private static Expression? CreateMapDecode(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, MapSchema targetSchema, MapSchema sourceSchema) =>
            GetDictionaryInterfaceDefinition(value.Type) switch
            {
                (bool isInterface, Type i, Type t) => ResolveDecode(assemblies, targetSchema.Values, sourceSchema.Values, stream, Expression.Parameter(t)) switch
                {
                    Expression read =>
                        Expression.Call(
                            stream,
                            isInterface switch
                            {
                                true =>
                                    typeof(IAvroDecoder).GetMethods()
                                    .First(r =>
                                        r.Name == nameof(IAvroDecoder.ReadMap) &&
                                        r.ContainsGenericParameters &&
                                        r.GetGenericArguments().Count() == 1
                                    )
                                    .MakeGenericMethod(t),
                                false =>
                                    typeof(IAvroDecoder).GetMethods()
                                    .First(r =>
                                        r.Name == nameof(IAvroDecoder.ReadMap) &&
                                        r.ContainsGenericParameters &&
                                        r.GetGenericArguments().Count() == 2 &&
                                        r.GetParameters().Count() == 1 &&
                                        r.ReturnType.Equals(r.GetGenericArguments()[0]) &&
                                        r.GetParameters()[0].ParameterType.Equals(typeof(Func<,>).MakeGenericType(typeof(IAvroDecoder), r.GetGenericArguments()[1]))
                                    )
                                    .MakeGenericMethod(value.Type, t)                                
                            },
                            Expression.Lambda(
                                typeof(Func<,>).MakeGenericType(typeof(IAvroDecoder), t),
                                read,
                                stream
                            )
                        ),
                    _ => default
                }
            };

        private static Expression CreateMapSkip(ParameterExpression stream, MapSchema sourceSchema) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipMap)),
                Expression.Lambda(
                    typeof(Action<>).MakeGenericType(typeof(IAvroDecoder)),
                    ResolveSkip(sourceSchema.Values, stream),
                    stream
                )
            )
        ;

        private static Expression? CreateEnumDecode(ParameterExpression stream, ParameterExpression value, EnumSchema targetSchema, EnumSchema sourceSchema) =>
            (targetSchema.Equals(sourceSchema), targetSchema.SequenceEqual(sourceSchema)) switch
            {
                (true, true) => GetEnumReader(stream, value, targetSchema),
                (true, false) =>
                (
                    GetEnumMappedReader(
                        stream,
                        value,
                        Expression.Variable(typeof(int)),
                        targetSchema,
                        sourceSchema
                    )
                ),
                _ => default
            };

        private static Expression CreateEnumSkip(ParameterExpression stream) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipEnum))
            )
        ;

        private static Expression GetEnumReader(ParameterExpression stream, ParameterExpression value, EnumSchema targetSchema) =>
            value.Type switch
            {
                { IsEnum: true } =>
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder)
                        .GetMethods()
                        .First(r =>
                            r.Name == nameof(IAvroDecoder.ReadEnum) &&
                            r.GetGenericArguments().Length == 1 &&
                            r.GetGenericArguments()[0]
                                .GetGenericParameterConstraints()
                                .SequenceEqual(new[] { typeof(Enum), typeof(ValueType) })
                        ).MakeGenericMethod(value.Type)
                    ),
                { IsClass: true } => value.Type.GetConstructor(new[] { typeof(EnumSchema) }) switch
                {
                    null => throw new ArgumentException($"{nameof(IAvroEnum)} implementation missing single argument '${nameof(EnumSchema)}' constructor."),
                    var c =>
                        Expression.Block(
                            value.Type,
                            new[] { value },
                            Expression.Assign(
                                value,
                                Expression.New(
                                    c,
                                    Expression.Constant(targetSchema)
                                )
                            ),
                            Expression.Call(
                                stream,
                                typeof(IAvroDecoder)
                                .GetMethods()
                                .First(r =>
                                    r.Name == nameof(IAvroDecoder.ReadEnum) &&
                                    r.GetGenericArguments().Length == 1 &&
                                    r.GetGenericArguments()[0]
                                        .GetGenericParameterConstraints()
                                        .Any(c => c.Equals(typeof(IAvroEnum)))
                                ).MakeGenericMethod(value.Type),
                                value
                            )
                        ),
                },
                _ => throw new Exception()
            };

        private static BlockExpression GetEnumMappedReader(ParameterExpression stream, ParameterExpression value, ParameterExpression indexParameter, EnumSchema targetSchema, EnumSchema sourceSchema) =>
            Expression.Block(
                value.Type,
                new List<ParameterExpression>()
                {
                    indexParameter,
                    value
                },
                Expression.Assign(
                    indexParameter,
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadInt))
                    )
                ),
                Expression.Switch(
                    typeof(void),
                    indexParameter,
                    Expression.Throw(
                        Expression.Constant(new IndexOutOfRangeException())
                    ),
                    null,
                    sourceSchema.Select(
                        (r, i) => GetEnumSwitchCase(
                            value,
                            i,
                            targetSchema,
                            sourceSchema
                        )
                    )
                    .Where(r => r != default)
                ),
                value
            );

        private static SwitchCase? GetEnumSwitchCase(ParameterExpression value, int i, EnumSchema targetSchema, EnumSchema sourceSchema) =>
            targetSchema.TryGetValue(sourceSchema[i], out var index) switch
            {
                true =>
                    Expression.SwitchCase(
                        Expression.Assign(
                            value,
                            (value.Type.IsEnum) switch
                            {
                                true =>
                                    Expression.Constant(
                                        Enum.Parse(value.Type, sourceSchema[index]),
                                        value.Type
                                    ),
                                _ =>
                                    Expression.New(
                                        typeof(GenericEnum).GetConstructor(new Type[] { typeof(EnumSchema), typeof(int) }),
                                        Expression.Constant(targetSchema),
                                        Expression.Constant(index)
                                    )
                            }
                        ),
                        Expression.Constant(i)
                    ),
                _ => default
            };

        private static Expression? CreateFixedDecode(ParameterExpression stream, ParameterExpression value, FixedSchema targetSchema, FixedSchema sourceSchema) =>
            (targetSchema.Equals(sourceSchema), typeof(IAvroFixed).IsAssignableFrom(value.Type)) switch
            {
                (true, true) =>
                    GetFixedReader(stream, Expression.Variable(value.Type), targetSchema)
                ,
                _ => default
            };

        private static Expression CreateFixedSkip(ParameterExpression stream, FixedSchema sourceSchema) =>
            Expression.Call(
                stream,
                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipFixed), new[] { typeof(int) }),
                Expression.Constant(sourceSchema.Size, typeof(int))
            )
        ;

        private static Expression GetFixedReader(ParameterExpression stream, ParameterExpression value, FixedSchema targetSchema) =>
            Expression.Block(
                value.Type,
                new[] { value },
                Expression.Assign(
                    value,
                    GetFixedInitializer(
                        value,
                        targetSchema
                    )
                ),
                value.Type switch
                {
                    var t when typeof(IAvroFixed).IsAssignableFrom(t) =>
                        Expression.Call(
                            stream,
                            typeof(IAvroDecoder)
                            .GetMethods()
                            .First(r =>
                                r.Name == nameof(IAvroDecoder.ReadFixed) &&
                                r.GetGenericArguments().Length == 1 &&
                                r.GetGenericArguments()[0]
                                    .GetGenericParameterConstraints()
                                    .Any(c => c.Equals(typeof(IAvroFixed)))
                            ).MakeGenericMethod(t),
                            value
                        ),
                    var t when t.Equals(typeof(byte[])) =>
                        Expression.Call(
                            stream,
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadFixed), new[] { typeof(byte[]) }),
                            value
                        ),
                    _ => default
                }
            );

        private static Expression GetFixedInitializer(ParameterExpression value, FixedSchema targetSchema) =>
            value.Type switch
            {
                var t when t.Equals(typeof(GenericFixed)) =>
                    Expression.New(
                        value.Type.GetConstructor(
                            new Type[] {
                                typeof(FixedSchema)
                            }
                        ),
                        Expression.Constant(
                            targetSchema,
                            typeof(FixedSchema)
                        )
                    ),
                var t when t.Equals(typeof(byte[])) =>
                    Expression.NewArrayBounds(
                        t,
                        Expression.Constant(
                            targetSchema.Size
                        )
                    ),
                _ =>
                    Expression.New(
                        value.Type.GetConstructor(Type.EmptyTypes)
                    ),
            };

        private static Expression CreateRecordSkip(ParameterExpression stream, RecordSchema sourceSchema) =>
            Expression.Block(
                typeof(void),
                new[] { stream },
                sourceSchema.Select(r => ResolveSkip(r, stream))
            )
        ;

        private static Expression CreateErrorSkip(ParameterExpression stream, ErrorSchema sourceSchema) =>
            Expression.Block(
                typeof(void),
                new[] { stream },
                sourceSchema.Select(r => ResolveSkip(r, stream))
            )
        ;

        private static IEnumerable<FieldSchema> GetRecordUnmappedFields(FieldsSchema targetSchema, FieldsSchema sourceSchema) =>
            targetSchema.Where(f => !sourceSchema.Any(w => w.Name == f.Name) && f.Default.Equals(JsonUtil.EmptyDefault));

        private static Expression GetErrorInitializer(ParameterExpression value, ErrorSchema targetSchema) =>
            value switch
            {
                var x when x.Type.Equals(typeof(GenericError)) =>
                    Expression.Assign(
                        value,
                        Expression.New(
                            typeof(GenericError).GetConstructor(
                                new Type[] {
                                    typeof(GenericError),
                                    typeof(bool)
                                }
                            ),
                            Expression.Constant(
                                new GenericError(targetSchema),
                                typeof(GenericError)
                            ),
                            Expression.Constant(
                                false,
                                typeof(bool)
                            )
                        )
                    ),
                _ =>
                    Expression.Assign(
                        value,
                        Expression.New(value.Type)
                    )
            };

        private static Expression GetRecordInitializer(ParameterExpression value, RecordSchema targetSchema) =>
            value switch
            {
                var x when x.Type.Equals(typeof(GenericRecord)) =>
                    Expression.Assign(
                        value,
                        Expression.New(
                            typeof(GenericRecord).GetConstructor(
                                new Type[] {
                                    typeof(GenericRecord),
                                    typeof(bool)
                                }
                            ),
                            Expression.Constant(
                                new GenericRecord(targetSchema),
                                typeof(GenericRecord)
                            ),
                            Expression.Constant(
                                false,
                                typeof(bool)
                            )
                        )
                    ),
                _ =>
                    Expression.Assign(
                        value,
                        Expression.New(value.Type)
                    )
            };

        private static IEnumerable<Expression?> GetFieldExpressions(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, FieldsSchema targetSchema, FieldsSchema sourceSchema) =>
            sourceSchema.Select(
                r => targetSchema.IndexOf(r.Name) switch
                {
                    -1 => ResolveSkip(r, stream),
                    var i => ResolveDecode(assemblies, targetSchema[i], r, stream, Expression.Variable(GetTypeFromSchema(targetSchema[i], assemblies))) switch
                    {
                        var read => value.Type switch
                        {
                            var t when t.Equals(typeof(GenericRecord)) || t.Equals(typeof(GenericError)) =>
                                Expression.Assign(
                                    Expression.MakeIndex(
                                        value,
                                        value.Type.GetProperty("Item", new[] { typeof(int) }),
                                        new[] { Expression.Constant(i) }
                                    ),
                                    Expression.Convert(
                                        read,
                                        typeof(object)
                                    )
                                ),
                            var t =>
                                Expression.Assign(
                                    Expression.MakeMemberAccess(
                                        value,
                                        value.Type.GetProperty(targetSchema[i].Name)
                                    ),
                                    read
                                )
                        }
                    }
                }
            );

        private static Expression GetErrorExpressions(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, ErrorSchema targetSchema, ErrorSchema sourceSchema) =>
            Expression.Block(
                value.Type,
                new ParameterExpression[] { value },
                GetFieldExpressions(stream, value, assemblies, targetSchema, sourceSchema)
                .Prepend(GetErrorInitializer(value, targetSchema))
                .Append(value)
            )
        ;

        private static Expression GetRecordExpressions(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, RecordSchema targetSchema, RecordSchema sourceSchema) =>
            Expression.Block(
                value.Type,
                new ParameterExpression[] { value },
                GetFieldExpressions(stream, value, assemblies, targetSchema, sourceSchema)
                .Prepend(GetRecordInitializer(value, targetSchema))
                .Append(value)
            )
        ;

        private static Expression CreateErrorDecode(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, ErrorSchema targetSchema, ErrorSchema sourceSchema) =>
            targetSchema.Equals(sourceSchema) switch
            {
                true => GetRecordUnmappedFields(targetSchema, sourceSchema) switch
                {
                    var x when x.Count() == 0 => GetErrorExpressions(stream, value, assemblies, targetSchema, sourceSchema),
                    var x => throw new ArgumentException($"Unmapped fields: '{string.Join(", ", x.Select(r => r.Name))}'")
                },
                _ => throw new ArgumentException("Error Source and Target Schema mismatch")
            };

        private static Expression CreateRecordDecode(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, RecordSchema targetSchema, RecordSchema sourceSchema) =>
            targetSchema.Equals(sourceSchema) switch
            {
                true => GetRecordUnmappedFields(targetSchema, sourceSchema) switch
                {
                    var x when x.Count() == 0 => GetRecordExpressions(stream, value, assemblies, targetSchema, sourceSchema),
                    var x => throw new ArgumentException($"Unmapped fields: '{string.Join(", ", x.Select(r => r.Name))}'")
                },
                _ => throw new ArgumentException("Record Source and Target Schema mismatch")
            };

        private static Expression CreateUnionSkip(ParameterExpression stream, UnionSchema sourceSchema) =>
            Expression.Switch(
                typeof(void),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadLong))
                ),
                Expression.Throw(
                    Expression.Constant(new IndexOutOfRangeException())
                ),
                default,
                sourceSchema.Select((r, i) => Expression.SwitchCase(ResolveSkip(r, stream), Expression.Constant((long)i)))
            )
        ;

        private static Expression? CreateUnionFromAnyDecode(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema targetSchema, AvroSchema sourceSchema) =>
            targetSchema switch
            {
                UnionSchema s when s.Count == 2 && s.NullIndex > -1 => GetNullableDecode(stream, value, assemblies, targetSchema, sourceSchema),
                _ => GetUnionDecode(stream, value, assemblies, targetSchema, sourceSchema)
            };

        private static Expression GetUnionDecode(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema targetSchema, AvroSchema sourceSchema) =>
            sourceSchema switch
            {
                UnionSchema s => s.Select((r, i) => GetUnionDecodeSwitchCase(stream, value, assemblies, targetSchema, r, i)) switch
                {
                    var c => Expression.Block(
                        value.Type,
                        new[] { value },
                        Expression.Switch(
                            typeof(void),
                            Expression.Call(
                                stream,
                                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadLong))
                            ),
                            Expression.Throw(
                                Expression.Constant(new IndexOutOfRangeException())
                            ),
                            default,
                            c.Where(r => r != default)
                        ),
                        value
                    )
                },
                var s => FindMatch(s, targetSchema) switch
                {
                    (-1, _) => throw new Exception(),
                    (var i, var t) => Expression.Convert(
                        ResolveDecode(assemblies, t, s, stream, Expression.Parameter(GetTypeFromSchema(t, assemblies))),
                        value.Type
                    )
                }
            };

        private static SwitchCase? GetUnionDecodeSwitchCase(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema targetSchema, AvroSchema sourceSchema, long index) =>
            FindMatch(sourceSchema, targetSchema) switch
            {
                (-1, _) => default,
                (_, var s) =>
                    Expression.SwitchCase(
                        Expression.Assign(
                            value,
                            Expression.Convert(
                                ResolveDecode(assemblies, s, sourceSchema, stream, Expression.Parameter(GetTypeFromSchema(s, assemblies))),
                                value.Type
                            )
                        ),
                        Expression.Constant(index)
                    )
            };

        /// <summary>
        /// Assumes target schema is a union with two schemas, one being null schema.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="value"></param>
        /// <param name="assemblies"></param>
        /// <param name="targetSchema"></param>
        /// <param name="sourceSchema"></param>
        /// <returns></returns>
        private static Expression? GetNullableDecode(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema targetSchema, AvroSchema sourceSchema) =>
            sourceSchema switch
            {
                NullSchema s => CreateNullDecode(stream, value),
                UnionSchema s when s.Count == 2 && s.NullIndex > -1 =>
                    Expression.Call(
                        stream,
                        Nullable.GetUnderlyingType(value.Type) == null ?
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadNullableObject)).MakeGenericMethod(value.Type) :
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadNullableValue)).MakeGenericMethod(Nullable.GetUnderlyingType(value.Type)),
                        Expression.Lambda(
                            ResolveDecode(assemblies, targetSchema[(targetSchema.NullIndex + 1) % 2], s[(s.NullIndex + 1) % 2], stream, Expression.Parameter(Nullable.GetUnderlyingType(value.Type) ?? value.Type)),
                            stream
                        ),
                        Expression.Constant((long)s.NullIndex)
                    ),
                UnionSchema s => s.Select((r, i) => GetNullableDecodeSwitchCase(stream, value, assemblies, targetSchema, r, i)) switch
                {
                    var c =>
                        Expression.Block(
                            value.Type,
                            new[] { value },
                            Expression.Switch(
                                typeof(void),
                                Expression.Call(
                                    stream,
                                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadLong))
                                ),
                                Expression.Throw(
                                    Expression.Constant(new IndexOutOfRangeException())
                                ),
                                default,
                                c.Where(r => r != default)
                            ),
                            value
                        )
                },
                AvroSchema s => ResolveDecode(assemblies, targetSchema[(targetSchema.NullIndex + 1) % 2], s, stream, value) switch
                {
                    Expression read =>
                        Expression.Block(
                            value.Type,
                            new[] { value },
                            Expression.Assign(
                                value,
                                read
                            ),
                            value
                        ),
                    _ => default
                }
            };

        private static SwitchCase? GetNullableDecodeSwitchCase(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema targetSchema, AvroSchema sourceSchema, long index) =>
            FindMatch(sourceSchema, targetSchema) switch
            {
                (-1, _) => default,
                (_, var s) =>
                    Expression.SwitchCase(
                        Expression.Assign(
                            value,
                            ResolveDecode(assemblies, s, sourceSchema, stream, value)
                        ),
                        Expression.Constant(
                            index
                        )
                    )
            };

        private static Expression CreateAnyFromUnionDecode(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, AvroSchema targetSchema, UnionSchema sourceSchema) =>
            sourceSchema.Select((r, i) => GetAnyFromUnionDecodeSwitchCase(stream, value, assemblies, targetSchema, r, i)) switch
            {
                var s when s.Where(r => r == default).Count() > 0 =>
                (
                    Expression.Block(
                        new[] { value },
                        Expression.Switch(
                            typeof(void),
                            Expression.Call(
                                stream,
                                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadLong))
                            ),
                            Expression.Throw(
                                Expression.Constant(
                                    new IndexOutOfRangeException()
                                )
                            ),
                            default,
                            s.Where(r => r != default)
                            .ToArray()
                        ),
                        value
                    )
                ),
                _ => throw new Exception()
            };

        private static SwitchCase? GetAnyFromUnionDecodeSwitchCase(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, AvroSchema targetSchema, AvroSchema sourceSchema, long index) =>
            ResolveDecode(assemblies, targetSchema, sourceSchema, stream, value) switch
            {
                Expression read =>
                    Expression.SwitchCase(
                        Expression.Assign(
                            value,
                            read
                        ),
                        Expression.Constant(
                            index
                        )
                    ),
                _ => default
            };
    }
}
