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
        private static ParameterExpression GetValueParameter(Type type) =>
            type switch
            {
                var t when t.IsGenericType && typeof(IDictionary<,>).Equals(type.GetGenericTypeDefinition()) => Expression.Parameter(t.GetGenericArguments().Last()),
                var t when t.IsGenericType && typeof(IList<>).Equals(type.GetGenericTypeDefinition()) => Expression.Parameter(t.GetGenericArguments().Last()),
                _ => Expression.Parameter(type)
            };

        private static Expression CastOrExpression(Expression expression, Type type)
        {
            if (type == null || type.Equals(expression.Type))
                return expression;
            return
                Expression.Convert(
                    expression,
                    type
                );


        }

        public static Type GetTypeFromSchema(AvroSchema schema, Assembly[] assemblies) =>
            schema switch
            {
                NullSchema r => typeof(AvroNull),
                BooleanSchema r => typeof(bool),
                IntSchema r => typeof(int),
                LongSchema r => typeof(long),
                FloatSchema r => typeof(float),
                DoubleSchema r => typeof(double),
                BytesSchema r => typeof(byte[]),
                StringSchema r => typeof(string),
                ArraySchema r => typeof(IList<>).MakeGenericType(GetTypeFromSchema(r.Items, assemblies)),
                MapSchema r => typeof(IDictionary<,>).MakeGenericType(typeof(string), GetTypeFromSchema(r.Values, assemblies)),
                DecimalSchema r => typeof(decimal),
                DateSchema r => typeof(DateTime),
                TimestampMillisSchema r => typeof(DateTime),
                TimestampMicrosSchema r => typeof(DateTime),
                TimestampNanosSchema r => typeof(DateTime),
                TimeMillisSchema r => typeof(TimeSpan),
                TimeMicrosSchema r => typeof(TimeSpan),
                TimeNanosSchema r => typeof(TimeSpan),
                DurationSchema r => typeof(AvroDuration),
                UuidSchema r => typeof(Guid),
                EnumSchema r => assemblies.SelectMany(m => m.GetTypes()).FirstOrDefault(t => t.FullName == r.FullName) ?? typeof(GenericEnum),
                FixedSchema r => assemblies.SelectMany(m => m.GetTypes()).FirstOrDefault(t => t.FullName == r.FullName) ?? typeof(GenericFixed),
                RecordSchema r => assemblies.SelectMany(m => m.GetTypes()).FirstOrDefault(t => t.FullName == r.FullName) ?? typeof(GenericRecord),
                UnionSchema r when r.Count == 2 && r.NullIndex > -1 =>
                    r[(r.NullIndex + 1) % 2] switch
                    {
                        IntSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        LongSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        FloatSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        DoubleSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        DecimalSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        DateSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        TimestampMillisSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        TimestampMicrosSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        TimestampNanosSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        TimeMillisSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        TimeMicrosSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        TimeNanosSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        UuidSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        EnumSchema v => typeof(Nullable<>).MakeGenericType(GetTypeFromSchema(v, assemblies)),
                        AvroSchema v => GetTypeFromSchema(v, assemblies),
                    },
                UnionSchema r => r.Count switch
                {
                    1 => typeof(AvroUnion<>).MakeGenericType(r.Select(s => GetTypeFromSchema(s, assemblies)).ToArray()),
                    2 => typeof(AvroUnion<,>).MakeGenericType(r.Select(s => GetTypeFromSchema(s, assemblies)).ToArray()),
                    3 => typeof(AvroUnion<,,>).MakeGenericType(r.Select(s => GetTypeFromSchema(s, assemblies)).ToArray()),
                    4 => typeof(AvroUnion<,,,>).MakeGenericType(r.Select(s => GetTypeFromSchema(s, assemblies)).ToArray()),
                    5 => typeof(AvroUnion<,,,,>).MakeGenericType(r.Select(s => GetTypeFromSchema(s, assemblies)).ToArray()),
                    var c => throw new ArgumentException($"Union schema invalid variant count: '{c}', must be [1:5]")
                },
                LogicalSchema r => GetTypeFromSchema(r.Type, assemblies),
                var r => throw new ArgumentException($"Unsupported schema: '{r.GetType().Name}'")
            };

        public static (int, AvroSchema) FindMatch(AvroSchema schema, IList<AvroSchema> schemas) =>
            schema switch
            {
                IntSchema s =>
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(IntSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(LongSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(FloatSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(DoubleSchema))),
                LongSchema s =>
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(LongSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(FloatSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(DoubleSchema))),
                FloatSchema s =>
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(DoubleSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(FloatSchema))),
                StringSchema s =>
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(StringSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(BytesSchema))),
                BytesSchema s =>
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(BytesSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(StringSchema))),
                TimeMillisSchema s =>
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimeMillisSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimeMicrosSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimeNanosSchema))),
                TimeMicrosSchema s =>
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimeMicrosSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimeNanosSchema))),
                TimestampMillisSchema s =>
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimestampMillisSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimestampMicrosSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimestampNanosSchema))),
                TimestampMicrosSchema s =>
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimestampMicrosSchema))) ??
                    schemas.FirstOrDefault(r => r.GetType().Equals(typeof(TimestampNanosSchema))),
                _ =>
                    schemas.FirstOrDefault(r => r.Equals(schema))
            }
            switch
            {
                null => (-1, EmptySchema.Value),
                var s => (schemas.IndexOf(s), s)
            };
    }
}
