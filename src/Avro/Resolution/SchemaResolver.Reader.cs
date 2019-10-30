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
            (var read, var skip) = ResolveDecoder(assemblies, targetSchema, sourceSchema, stream, value);
            if (read == null)
                throw new AvroException($"Unable to resolve reader: '{targetSchema}' using writer: '{sourceSchema}' for type: '{type}'");

            return (
                Expression.Lambda<Func<IAvroDecoder, T>>(
                    read.CanReduce ? read.Reduce() : read,
                    stream
                ).Compile(),
                Expression.Lambda<Action<IAvroDecoder>>(
                    skip,
                    stream
                ).Compile()
             );
        }

        private static (Expression, Expression) ResolveDecoder(Assembly[] assemblies, AvroSchema targetSchema, AvroSchema sourceSchema, ParameterExpression stream, ParameterExpression value) =>
            (targetSchema, sourceSchema) switch
            {
                (NullSchema _, NullSchema _) => CreateNullDecoder(stream, value),
                (BooleanSchema _, BooleanSchema _) => CreateBooleanDecoder(stream, value),
                (IntSchema _, IntSchema _) => CreateIntDecoder(stream, value),
                (LongSchema _, LongSchema _) => CreateLongDecoder(stream, value),
                (LongSchema _, IntSchema _) => CreateIntDecoder(stream, value),
                (FloatSchema _, FloatSchema _) => CreateFloatDecoder(stream, value),
                (FloatSchema _, LongSchema _) => CreateLongDecoder(stream, value),
                (FloatSchema _, IntSchema _) => CreateIntDecoder(stream, value),
                (DoubleSchema _, DoubleSchema _) => CreateDoubleDecoder(stream, value),
                (DoubleSchema _, FloatSchema _) => CreateFloatDecoder(stream, value),
                (DoubleSchema _, LongSchema _) => CreateLongDecoder(stream, value),
                (DoubleSchema _, IntSchema _) => CreateIntDecoder(stream, value),
                (BytesSchema _, BytesSchema _) => CreateBytesDecoder(stream, value),
                (BytesSchema _, StringSchema _) => CreateBytesDecoder(stream, value),
                (StringSchema _, StringSchema _) => CreateStringDecoder(stream, value),
                (StringSchema _, BytesSchema _) => CreateStringDecoder(stream, value),
                (UuidSchema _, UuidSchema _) => CreateUuidDecoder(stream, value),
                (DateSchema _, DateSchema _) => CreateDateDecoder(stream, value),
                (TimeMillisSchema _, TimeMillisSchema _) => CreateTimeMsDecoder(stream, value),
                (TimeMicrosSchema _, TimeMicrosSchema _) => CreateTimeUsDecoder(stream, value),
                (TimeMicrosSchema _, TimeMillisSchema _) => CreateTimeMsDecoder(stream, value),
                (TimeNanosSchema _, TimeNanosSchema _) => CreateTimeNsDecoder(stream, value),
                (TimeNanosSchema _, TimeMicrosSchema _) => CreateTimeUsDecoder(stream, value),
                (TimeNanosSchema _, TimeMillisSchema _) => CreateTimeMsDecoder(stream, value),
                (TimestampMillisSchema _, TimestampMillisSchema _) => CreateTimestampMsDecoder(stream, value),
                (TimestampMicrosSchema _, TimestampMicrosSchema _) => CreateTimestampUsDecoder(stream, value),
                (TimestampMicrosSchema _, TimestampMillisSchema _) => CreateTimestampMsDecoder(stream, value),
                (TimestampNanosSchema _, TimestampNanosSchema _) => CreateTimestampNsDecoder(stream, value),
                (TimestampNanosSchema _, TimestampMicrosSchema _) => CreateTimestampUsDecoder(stream, value),
                (TimestampNanosSchema _, TimestampMillisSchema _) => CreateTimestampMsDecoder(stream, value),
                (DurationSchema _, DurationSchema _) => CreateDurationDecoder(stream, value),
                (DecimalSchema t, DecimalSchema s) => CreateDecimalDecoder(stream, value, t, s),
                (ArraySchema t, ArraySchema s) => CreateArrayDecoder(stream, value, assemblies, t, s),
                (MapSchema t, MapSchema s) => CreateMapDecoder(stream, value, assemblies, t, s),
                (EnumSchema t, EnumSchema s) => CreateEnumDecoder(stream, value, t, s),
                (FixedSchema t, FixedSchema s) => CreateFixedDecoder(stream, value, t, s),
                (RecordSchema t, RecordSchema s) => CreateRecordDecoder(stream, value, assemblies, t, s),
                (UnionSchema t, AvroSchema s) => CreateUnionFromAnyDecoder(stream, value, assemblies, t, s),
                (AvroSchema t, UnionSchema s) => CreateAnyFromUnionDecoder(stream, value, assemblies, t, s),
                (_, _) => default,
            };

        private static (Expression, Expression) CreateNullDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
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
                },
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipNull))
                )
            );

        private static (Expression, Expression) CreateBooleanDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadBoolean))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipBoolean))
                )
            );

        private static (Expression, Expression) CreateIntDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadInt))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipInt))
                )
            );

        private static (Expression, Expression) CreateLongDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadLong))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipLong))
                )
            );

        private static (Expression, Expression) CreateFloatDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadFloat))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipFloat))
                )
            );

        private static (Expression, Expression) CreateDoubleDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadDouble))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipDouble))
                )
            );

        private static (Expression, Expression) CreateBytesDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
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
                },
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipBytes)),
                    null
                )
            );

        private static (Expression, Expression) CreateStringDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
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
                },
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipString))
                )
            );

        private static (Expression, Expression) CreateUuidDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadUuid))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipUuid))
                )
            );

        private static (Expression, Expression) CreateDateDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadDate))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipDate))
                )
            );

        private static (Expression, Expression) CreateTimeMsDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadTimeMS))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipTimeMS))
                )
            );

        private static (Expression, Expression) CreateTimeUsDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadTimeUS))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipTimeUS))
                )
            );

        private static (Expression, Expression) CreateTimeNsDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadTimeNS))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipTimeNS))
                )
            );

        private static (Expression, Expression) CreateTimestampMsDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadTimestampMS))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipTimestampMS))
                )
            );

        private static (Expression, Expression) CreateTimestampUsDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadTimestampUS))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipTimestampUS))
                )
            );

        private static (Expression, Expression) CreateTimestampNsDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadTimestampNS))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipTimestampNS))
                )
            );

        private static (Expression, Expression) CreateDurationDecoder(ParameterExpression stream, ParameterExpression value) =>
            (
                CastOrExpression(
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadDuration))
                    ),
                    value.Type
                ),
                Expression.Call(
                    stream,
                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipDuration))
                )
            );

        private static (Expression, Expression) CreateDecimalDecoder(ParameterExpression stream, ParameterExpression value, DecimalSchema targetSchema, DecimalSchema sourceSchema) =>
            (targetSchema.Equals(sourceSchema), sourceSchema.Type) switch
            {
                (true, BytesSchema s) =>
                (
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadDecimal), new Type[] { typeof(int) }),
                        Expression.Constant(targetSchema.Scale)
                    ),
                    Expression.Call(
                            stream,
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipDecimal), new Type[] { })
                    )
                ),
                (true, FixedSchema s) =>
                (
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadDecimal), new Type[] { typeof(int), typeof(int) }),
                        Expression.Constant(targetSchema.Scale),
                        Expression.Constant(s.Size)
                    ),
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipDecimal), new Type[] { typeof(int) }),
                        Expression.Constant(s.Size)
                    )
                ),
                _ => default
            };

        private static (Expression, Expression) CreateArrayDecoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, ArraySchema targetSchema, ArraySchema sourceSchema) =>
            ResolveDecoder(assemblies, targetSchema.Items, sourceSchema.Items, stream, Expression.Parameter(value.Type.GetGenericArguments().Last())) switch
            {
                (Expression read, Expression skip) =>
                (
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadArray)).MakeGenericMethod(value.Type.GetGenericArguments().Last()),
                        Expression.Lambda(
                            typeof(Func<,>).MakeGenericType(typeof(IAvroDecoder), value.Type.GetGenericArguments().Last()),
                            read,
                            stream
                        )
                    ),
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipArray)),
                        Expression.Lambda(
                            typeof(Action<>).MakeGenericType(typeof(IAvroDecoder)),
                            skip,
                            stream
                        )
                    )
                ),
                _ => default
            };

        private static (Expression, Expression) CreateMapDecoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, MapSchema targetSchema, MapSchema sourceSchema) =>
            ResolveDecoder(assemblies, targetSchema.Values, sourceSchema.Values, stream, Expression.Parameter(value.Type.GetGenericArguments().Last())) switch
            {
                (Expression read, Expression skip) =>
                (
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadMap)).MakeGenericMethod(value.Type.GetGenericArguments().Last()),
                        Expression.Lambda(
                            typeof(Func<,>).MakeGenericType(typeof(IAvroDecoder), value.Type.GetGenericArguments().Last()),
                            read,
                            stream
                        )
                    ),
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipMap)),
                        Expression.Lambda(
                            typeof(Action<>).MakeGenericType(typeof(IAvroDecoder)),
                            skip,
                            stream
                        )
                    )
                ),
                _ => default
            };

        private static (Expression, Expression) CreateEnumDecoder(ParameterExpression stream, ParameterExpression value, EnumSchema targetSchema, EnumSchema sourceSchema) =>
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
                    ),
                    Expression.Call(
                        stream,
                        typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipInt))
                    )
                ),
                _ => default
            };

        private static (Expression, Expression) GetEnumReader(ParameterExpression stream, ParameterExpression value, EnumSchema targetSchema) =>
            value.Type switch
            {
                { IsEnum: true } =>
                    (
                        Expression.Call(
                            stream,
                            typeof(IAvroDecoder)
                            .GetMethods()
                            .First(
                                r => r.Name == nameof(IAvroDecoder.ReadEnum) &&
                                     r.GetGenericArguments().Length == 1 &&
                                     r.GetGenericArguments()[0]
                                        .GetGenericParameterConstraints()
                                        .SequenceEqual(new[] { typeof(Enum), typeof(ValueType) })
                            ).MakeGenericMethod(value.Type)
                        ),
                        Expression.Call(
                            stream,
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipEnum))
                        )
                    ),
                { IsClass: true } => value.Type.GetConstructor(new[] { typeof(EnumSchema) }) switch
                {
                    null => throw new ArgumentException($"{nameof(IAvroEnum)} implementation missing single argument '${nameof(EnumSchema)}' constructor."),
                    var c => (
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
                                .First(
                                    r => r.Name == nameof(IAvroDecoder.ReadEnum) &&
                                         r.GetGenericArguments().Length == 1 &&
                                         r.GetGenericArguments()[0]
                                            .GetGenericParameterConstraints()
                                            .Any(c => c.Equals(typeof(IAvroEnum)))
                                ).MakeGenericMethod(value.Type),
                                value
                            )
                        ),
                        Expression.Call(
                            stream,
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipEnum))
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

        private static (Expression, Expression) CreateFixedDecoder(ParameterExpression stream, ParameterExpression value, FixedSchema targetSchema, FixedSchema sourceSchema) =>
            (targetSchema.Equals(sourceSchema), typeof(IAvroFixed).IsAssignableFrom(value.Type)) switch
            {
                (true, true) =>
                    (
                        GetFixedReader(stream, Expression.Variable(value.Type), targetSchema),
                        Expression.Call(
                            stream,
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipFixed), new[] { typeof(int) }),
                            Expression.Constant(targetSchema.Size, typeof(int))
                        )
                    ),
                _ => default
            };

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
                            .First(
                                r => r.Name == nameof(IAvroDecoder.ReadFixed) &&
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

        private static IEnumerable<RecordFieldSchema> GetRecordUnmappedFields(RecordSchema targetSchema, RecordSchema sourceSchema) =>
            targetSchema.Where(f => !sourceSchema.Any(w => w.Name == f.Name)) switch
            {
                var x when x.Count(f => f.Default.Equals(JsonUtil.EmptyDefault)) > 0 =>
                    throw new AvroException($"Unmapped field without default: '{string.Join(", ", x.Select(f => f.Name))}'"),
                var x => x
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

        private static IEnumerable<(Expression, Expression)> GetRecordFieldExpressions(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, RecordSchema targetSchema, RecordSchema sourceSchema) =>
            sourceSchema.Select(
                r => targetSchema.IndexOf(r.Name) switch
                {
                    -1 => ResolveDecoder(assemblies, r.Type, r.Type, stream, Expression.Variable(GetTypeFromSchema(r.Type, assemblies))) switch
                    {
                        (Expression read, Expression skip) => (skip, skip)
                    },
                    var i => ResolveDecoder(assemblies, targetSchema[i].Type, r.Type, stream, Expression.Variable(GetTypeFromSchema(targetSchema[i].Type, assemblies))) switch
                    {
                        (var read, var skip) => value.Type switch
                        {
                            var t when typeof(GenericRecord).IsAssignableFrom(t) =>
                                (
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
                                    skip
                                ),
                            var t =>
                                (
                                    Expression.Assign(
                                        Expression.MakeMemberAccess(
                                            value,
                                            value.Type.GetProperty(targetSchema[i].Name)
                                        ),
                                        read
                                    ),
                                    skip
                                )
                        }
                    }
                }
            );

        private static (Expression, Expression) GetRecordExpressions(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, RecordSchema targetSchema, RecordSchema sourceSchema, IEnumerable<RecordFieldSchema> defaults) =>
            GetRecordFieldExpressions(stream, value, assemblies, targetSchema, sourceSchema) switch
            {
                var x =>
                (
                    Expression.Block(
                        value.Type,
                        new ParameterExpression[] { value },
                        x.Select(r => r.Item1)
                        .Prepend(GetRecordInitializer(value, targetSchema))
                        .Append(value)
                    ),
                    Expression.Block(
                        x.Select(r => r.Item2)
                    )
                )
            };

        private static (Expression, Expression) CreateRecordDecoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, RecordSchema targetSchema, RecordSchema sourceSchema) =>
            targetSchema.Equals(sourceSchema) switch
            {
                true => GetRecordExpressions(stream, value, assemblies, targetSchema, sourceSchema, GetRecordUnmappedFields(targetSchema, sourceSchema)),
                _ => throw new ArgumentException("Record Source and Target Schema mismatch")
            };

        private static (Expression, Expression) CreateUnionFromAnyDecoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema targetSchema, AvroSchema sourceSchema) =>
            targetSchema switch
            {
                UnionSchema s when s.Count == 2 && s.NullIndex > -1 => GetNullableDecoder(stream, value, assemblies, targetSchema, sourceSchema),
                _ => GetUnionDecoder(stream, value, assemblies, targetSchema, sourceSchema)
            };

        private static (Expression, Expression) GetUnionDecoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema targetSchema, AvroSchema sourceSchema) =>
            sourceSchema switch
            {
                UnionSchema s => s.Select((r, i) => GetUnionDecodeSwitchCase(stream, value, assemblies, targetSchema, r, i)) switch
                {
                    var c =>
                        (
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
                                    c.Where(r => r.Item1 != default)
                                    .Select(r => r.Item1)
                                ),
                                value
                            ),
                            Expression.Switch(
                                Expression.Call(
                                    stream,
                                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadLong))
                                ),
                                c.Select(r => r.Item2).ToArray()
                            )
                        )
                },
                var s => FindMatch(s, targetSchema) switch
                {
                    (-1, _) => throw new Exception(),
                    (var i, var t) => ResolveDecoder(assemblies, t, s, stream, Expression.Parameter(GetTypeFromSchema(t, assemblies))) switch
                    {
                        (Expression read, Expression skip) =>
                        (
                            Expression.Convert(
                                read,
                                value.Type
                            ),
                            skip
                        )
                    }
                }
            };

        private static (SwitchCase, SwitchCase) GetUnionDecodeSwitchCase(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema targetSchema, AvroSchema sourceSchema, long index) =>
            FindMatch(sourceSchema, targetSchema) switch
            {
                (-1, _) => ResolveDecoder(assemblies, sourceSchema, sourceSchema, stream, Expression.Parameter(GetTypeFromSchema(sourceSchema, assemblies))) switch
                {
                    (_, var skip) =>
                    (
                        default,
                        Expression.SwitchCase(
                            skip,
                            Expression.Constant(index)
                        )
                    )
                },
                (_, var s) => ResolveDecoder(assemblies, s, sourceSchema, stream, Expression.Parameter(GetTypeFromSchema(s, assemblies))) switch
                {
                    (Expression read, Expression skip) =>
                    (
                        Expression.SwitchCase(
                            Expression.Assign(
                                value,
                                Expression.Convert(
                                    read,
                                    value.Type
                                )
                            ),
                            Expression.Constant(index)
                        ),
                        Expression.SwitchCase(
                            skip,
                            Expression.Constant(index)
                        )
                    )
                }
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
        private static (Expression, Expression) GetNullableDecoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema targetSchema, AvroSchema sourceSchema) =>
            sourceSchema switch
            {
                NullSchema s => CreateNullDecoder(stream, value),
                UnionSchema s when s.Count == 2 && s.NullIndex > -1 =>
                    ResolveDecoder(assemblies, targetSchema[(targetSchema.NullIndex + 1) % 2], s[(s.NullIndex + 1) % 2], stream, Expression.Parameter(Nullable.GetUnderlyingType(value.Type) ?? value.Type)) switch
                    {
                        (Expression read, Expression skip) =>
                        (
                            Expression.Call(
                                stream,
                                Nullable.GetUnderlyingType(value.Type) == null ?
                                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadNullableObject)).MakeGenericMethod(value.Type) :
                                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadNullableValue)).MakeGenericMethod(Nullable.GetUnderlyingType(value.Type)),
                                Expression.Lambda(
                                    read,
                                    stream
                                ),
                                Expression.Constant((long)s.NullIndex)
                            ),
                            Expression.Call(
                                stream,
                                typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.SkipNullable)),
                                Expression.Lambda(
                                    skip,
                                    stream
                                ),
                                Expression.Constant((long)s.NullIndex)
                            )
                        )
                    },
                UnionSchema s => s.Select((r, i) => GetNullableDecodeSwitchCase(stream, value, assemblies, targetSchema, r, i)) switch
                {
                    var c =>
                        (
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
                                    c.Where(r => r.Item1 != default)
                                    .Select(r => r.Item1)
                                ),
                                value
                            ),
                            Expression.Switch(
                                Expression.Call(
                                    stream,
                                    typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadLong))
                                ),
                                c.Select(r => r.Item2).ToArray()
                            )
                        )
                },
                AvroSchema s => ResolveDecoder(assemblies, targetSchema[(targetSchema.NullIndex + 1) % 2], s, stream, value) switch
                {
                    (Expression read, Expression skip) =>
                    (
                        Expression.Block(
                            value.Type,
                            new[] { value },
                            Expression.Assign(
                                value,
                                read
                            ),
                            value
                        ),
                        skip
                    )
                }
            };

        private static (SwitchCase, SwitchCase) GetNullableDecodeSwitchCase(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, UnionSchema targetSchema, AvroSchema sourceSchema, long index) =>
            FindMatch(sourceSchema, targetSchema) switch
            {
                (-1, _) => ResolveDecoder(assemblies, sourceSchema, sourceSchema, stream, Expression.Parameter(GetTypeFromSchema(sourceSchema, assemblies))) switch
                {
                    (_, var skip) =>
                    (
                        default,
                        Expression.SwitchCase(
                            skip,
                            Expression.Constant(
                               index
                            )
                        )
                    )
                },
                (_, var s) => ResolveDecoder(assemblies, s, sourceSchema, stream, value) switch
                {
                    (Expression read, Expression skip) =>
                    (
                        Expression.SwitchCase(
                            Expression.Assign(
                                value,
                                read
                            ),
                            Expression.Constant(
                                index
                            )
                        ),
                        Expression.SwitchCase(
                            skip,
                            Expression.Constant(
                               index
                            )
                        )
                    )
                }
            };

        private static (Expression, Expression) CreateAnyFromUnionDecoder(ParameterExpression stream, ParameterExpression value, Assembly[] assemblies, AvroSchema readSchema, IEnumerable<AvroSchema> writeSchemas)
        {
            var unionToNonUnionReadCases = new SwitchCase[writeSchemas.Count()];
            var unionToNonUnionSkipCases = new SwitchCase[writeSchemas.Count()];
            var unionToNonUnionTypeIndex =
                Expression.Variable(
                    typeof(long),
                    "unionTypeIndex"
                );

            for (int i = 0; i < writeSchemas.Count(); i++)
            {
                (var read, var skip) = ResolveDecoder(assemblies, readSchema, writeSchemas.ElementAt(i), stream, value);
                if (read != null)
                {
                    unionToNonUnionReadCases[i] =
                        Expression.SwitchCase(
                            Expression.Assign(
                                value,
                                Expression.Convert(
                                    read,
                                    value.Type
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
                    var skipParameter = Expression.Variable(GetTypeFromSchema(writeSchemas.ElementAt(i), assemblies));
                    (read, skip) = ResolveDecoder(assemblies, writeSchemas.ElementAt(i), writeSchemas.ElementAt(i), stream, skipParameter);
                }

                unionToNonUnionSkipCases[i] =
                    Expression.SwitchCase(
                        skip,
                        Expression.Constant(
                            (long)i,
                            typeof(long)
                        )
                    );
            }

            return (
                Expression.Block(
                    value.Type,
                    new List<ParameterExpression>()
                    {
                        unionToNonUnionTypeIndex,
                        value
                    },
                    Expression.Assign(
                        unionToNonUnionTypeIndex,
                        Expression.Call(
                            stream,
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadLong))
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
                    value
                ),
                Expression.Block(
                    new List<ParameterExpression>()
                    {
                                unionToNonUnionTypeIndex
                    },
                    Expression.Assign(
                        unionToNonUnionTypeIndex,
                        Expression.Call(
                            stream,
                            typeof(IAvroDecoder).GetMethod(nameof(IAvroDecoder.ReadLong))
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
