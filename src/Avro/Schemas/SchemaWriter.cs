using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Avro.Schemas
{
    public static class SchemaWriter
    {
        public static string ToString(this Schema schema, WriterMode mode)
        {
            var sb = new StringBuilder();
            using (var sw = new StringWriter(sb))
                Write(sw, schema, mode, new HashSet<string>());
            return sb.ToString();
        }

        public static void Write(TextWriter writer, Schema schema, ISet<string> namedSchemas = null)
        {
            if (namedSchemas == null)
                namedSchemas = new HashSet<string>();
            Write(writer, schema, WriterMode.None, namedSchemas);
        }

        public static void WriteFull(TextWriter writer, Schema schema, ISet<string> namedSchemas = null)
        {
            if (namedSchemas == null)
                namedSchemas = new HashSet<string>();
            Write(writer, schema, WriterMode.Full, namedSchemas);
        }

        public static void WriteCanonical(TextWriter writer, Schema schema, ISet<string> namedSchemas = null)
        {
            if (namedSchemas == null)
                namedSchemas = new HashSet<string>();
            Write(writer, schema, WriterMode.Canonical, namedSchemas);
        }

        private static void Write(TextWriter writer, Schema schema, WriterMode mode, ISet<string> namedSchemas)
        {
            if (schema is NamedSchema)
            {
                var namedSchema = schema as NamedSchema;
                if (namedSchemas.Contains(namedSchema.FullName))
                {
                    writer.Write(@"""");
                    writer.Write(namedSchema.FullName);
                    writer.Write(@"""");
                    return;
                }
                namedSchemas.Add(namedSchema.FullName);
            }

            switch (schema.GetType().Name)
            {
                case nameof(NullSchema):
                case nameof(BooleanSchema):
                case nameof(IntSchema):
                case nameof(LongSchema):
                case nameof(FloatSchema):
                case nameof(DoubleSchema):
                case nameof(BytesSchema):
                case nameof(StringSchema):
                    WritePrimitive(writer, schema, mode);
                    break;

                case nameof(DateSchema):
                case nameof(TimeMillisSchema):
                case nameof(TimeMicrosSchema):
                case nameof(TimestampMillisSchema):
                case nameof(TimestampMicrosSchema):
                case nameof(DurationSchema):
                case nameof(UuidSchema):
                    WriteLogicalType(writer, schema as LogicalSchema, mode, namedSchemas);
                    break;

                case nameof(ArraySchema):
                    WriteArray(writer, schema as ArraySchema, mode, namedSchemas);
                    break;
                case nameof(MapSchema):
                    WriteMap(writer, schema as MapSchema, mode, namedSchemas);
                    break;

                case nameof(UnionSchema):
                    WriteUnion(writer, schema as UnionSchema, mode, namedSchemas);
                    break;

                case nameof(FixedSchema):
                    WriteFixed(writer, schema as FixedSchema, mode);
                    break;
                case nameof(EnumSchema):
                    WriteEnum(writer, schema as EnumSchema, mode);
                    break;
                case nameof(RecordSchema):
                    WriteRecord(writer, schema as RecordSchema, mode, namedSchemas);
                    break;
                case nameof(ErrorSchema):
                    WriteError(writer, schema as ErrorSchema, mode, namedSchemas);
                    break;

                case nameof(DecimalSchema):
                    WriteDecimal(writer, schema as DecimalSchema, mode, namedSchemas);
                    break;

                default:
                    if(schema is LogicalSchema)
                        WriteLogicalType(writer, schema as LogicalSchema, mode, namedSchemas);
                    else
                        throw new AvroException($"Unknown schema type: '{schema.GetType().FullName}'");
                    break;
            }
        }

        private static void WritePrimitive(TextWriter writer, Schema schema, WriterMode mode)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"""");
                    writer.Write(schema.ToString());
                    writer.Write(@"""");
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ""type"": """);
                    writer.Write(schema.ToString());
                    writer.Write(@""" }");
                    break;
                default:
                    writer.Write(@"""");
                    writer.Write(schema.ToString());
                    writer.Write(@"""");
                    break;
            }
        }

        private static void WriteArray(TextWriter writer, ArraySchema schema, WriterMode mode, ISet<string> namedSchemas)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""type"":""array"",""items"":");
                    Write(writer, schema.Items, mode, namedSchemas);
                    writer.Write(@"}");
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ""type"": ""array"", ""items"": ");
                    Write(writer, schema.Items, mode, namedSchemas);
                    writer.Write(@" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ""array"", ""items"": ");
                    Write(writer, schema.Items, mode, namedSchemas);
                    writer.Write(@" }");
                    break;
            }
        }

        private static void WriteMap(TextWriter writer, MapSchema schema, WriterMode mode, ISet<string> namedSchemas)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""type"":""map"",""values"":");
                    Write(writer, schema.Values, mode, namedSchemas);
                    writer.Write(@"}");
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ""type"": ""map"", ""values"": ");
                    Write(writer, schema.Values, mode, namedSchemas);
                    writer.Write(@" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ""map"", ""values"": ");
                    Write(writer, schema.Values, mode, namedSchemas);
                    writer.Write(@" }");
                    break;
            }
        }

        private static void WriteUnion(TextWriter writer, UnionSchema schema, WriterMode mode, ISet<string> namedSchemas)
        {
            var i = 0;
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"[");
                    foreach (var item in schema)
                    {
                        if (i++ > 0)
                            writer.Write(",");
                        Write(writer, item, mode, namedSchemas);
                    }
                    writer.Write(@"]");
                    break;
                case WriterMode.Full:
                    writer.Write(@"[");
                    foreach (var item in schema)
                    {
                        if (i++ > 0)
                            writer.Write(", ");
                        Write(writer, item, mode, namedSchemas);
                    }
                    writer.Write(@"]");
                    break;
                default:
                    writer.Write(@"[");
                    foreach (var item in schema)
                    {
                        if (i++ > 0)
                            writer.Write(", ");
                        Write(writer, item, mode, namedSchemas);
                    }
                    writer.Write(@"]");
                    break;
            }
        }

        private static void WriteFixed(TextWriter writer, FixedSchema schema, WriterMode mode)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""name"":""");
                    writer.Write(schema.FullName);
                    writer.Write(@"""");
                    writer.Write(@",""type"":""fixed""");
                    writer.Write(@",""size"":");
                    writer.Write(schema.Size);
                    writer.Write(@"}");
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ""type"": ""fixed""");
                    writer.Write(@", ""name"": """);
                    writer.Write(schema.Name);
                    writer.Write(@"""");
                    writer.Write(@", ""namespace"": """);
                    writer.Write(schema.Namespace);
                    writer.Write(@"""");
                    writer.Write(@", ""size"": ");
                    writer.Write(schema.Size);
                    writer.Write(@", ""aliases"": [");
                    writer.Write(string.Join(", ", schema.Aliases.Select(r => $"\"{r}\"")));
                    writer.Write(@"]");
                    writer.Write(@" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ""fixed""");
                    writer.Write(@", ""name"": """);
                    writer.Write(schema.Name);
                    writer.Write(@"""");
                    if (!string.IsNullOrEmpty(schema.Namespace))
                    {
                        writer.Write(@", ""namespace"": """);
                        writer.Write(schema.Namespace);
                        writer.Write(@"""");
                    }
                    writer.Write(@", ""size"": ");
                    writer.Write(schema.Size);
                    if (schema.Aliases.Count() > 0)
                    {
                        writer.Write(@", ""aliases"": [");
                        writer.Write(string.Join(", ", schema.Aliases.Select(r => $"\"{r}\"")));
                        writer.Write(@"]");
                    }
                    writer.Write(@" }");
                    break;
            }
        }

        private static void WriteEnum(TextWriter writer, EnumSchema schema, WriterMode mode)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""name"":""");
                    writer.Write(schema.FullName);
                    writer.Write(@"""");
                    writer.Write(@",""type"":""enum""");
                    writer.Write(@",""symbols"":[");
                    writer.Write(string.Join(",", schema.Symbols.Select(r => $"\"{r}\"")));
                    writer.Write(@"]");
                    writer.Write(@"}");
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ""type"": ""enum""");
                    writer.Write(@", ""name"": """);
                    writer.Write(schema.Name);
                    writer.Write(@"""");
                    writer.Write(@", ""namespace"": """);
                    writer.Write(schema.Namespace);
                    writer.Write(@"""");
                    writer.Write(@", ""symbols"": [");
                    writer.Write(string.Join(", ", schema.Symbols.Select(r => $"\"{r}\"")));
                    writer.Write(@"]");
                    writer.Write(@", ""doc"": """);
                    writer.Write(schema.Doc);
                    writer.Write(@"""");
                    writer.Write(@", ""aliases"": [");
                    writer.Write(string.Join(", ", schema.Aliases.Select(r => $"\"{r}\"")));
                    writer.Write(@"]");
                    writer.Write(@" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ""enum""");
                    writer.Write(@", ""name"": """);
                    writer.Write(schema.Name);
                    writer.Write(@"""");
                    if (!string.IsNullOrEmpty(schema.Namespace))
                    {
                        writer.Write(@", ""namespace"": """);
                        writer.Write(schema.Namespace);
                        writer.Write(@"""");
                    }
                    writer.Write(@", ""symbols"": [");
                    writer.Write(string.Join(", ", schema.Symbols.Select(r => $"\"{r}\"")));
                    writer.Write(@"]");
                    if (!string.IsNullOrEmpty(schema.Doc))
                    {
                        writer.Write(@", ""doc"": """);
                        writer.Write(schema.Doc);
                        writer.Write(@"""");
                    }
                    if (schema.Aliases.Count > 0)
                    {
                        writer.Write(@", ""aliases"": [");
                        writer.Write(string.Join(", ", schema.Aliases.Select(r => $"\"{r}\"")));
                        writer.Write(@"]");
                    }
                    writer.Write(@" }");
                    break;
            }
        }


        private static void WriteRecord(TextWriter writer, RecordSchema schema, WriterMode mode, ISet<string> namedSchemas)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""name"":""");
                    writer.Write(schema.FullName);
                    writer.Write(@"""");
                    writer.Write(@",""type"":""record""");
                    writer.Write(@",""fields"":[");
                    WriteFields(writer, schema, mode, namedSchemas);
                    writer.Write(@"]");
                    writer.Write(@"}");
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ""type"": ""record""");
                    writer.Write(@", ""name"": """);
                    writer.Write(schema.Name);
                    writer.Write(@"""");
                    writer.Write(@", ""namespace"": """);
                    writer.Write(schema.Namespace);
                    writer.Write(@"""");
                    writer.Write(@", ""doc"": """);
                    writer.Write(schema.Doc);
                    writer.Write(@"""");
                    writer.Write(@", ""aliases"": [");
                    writer.Write(string.Join(", ", schema.Aliases.Select(r => $"\"{r}\"")));
                    writer.Write(@"]");
                    writer.Write(@", ""fields"": [");
                    WriteFields(writer, schema, mode, namedSchemas);
                    writer.Write(@"]");
                    writer.Write(@" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ""record""");
                    writer.Write(@", ""name"": """);
                    writer.Write(schema.Name);
                    writer.Write(@"""");
                    if (!string.IsNullOrEmpty(schema.Namespace))
                    {
                        writer.Write(@", ""namespace"": """);
                        writer.Write(schema.Namespace);
                        writer.Write(@"""");
                    }
                    if (!string.IsNullOrEmpty(schema.Doc))
                    {
                        writer.Write(@", ""doc"": """);
                        writer.Write(schema.Doc);
                        writer.Write(@"""");
                    }
                    if (schema.Aliases.Count > 0)
                    {
                        writer.Write(@", ""aliases"": [");
                        writer.Write(string.Join(", ", schema.Aliases.Select(r => $"\"{r}\"")));
                        writer.Write(@"]");
                    }
                    writer.Write(@", ""fields"": [");
                    WriteFields(writer, schema, mode, namedSchemas);
                    writer.Write(@"]");
                    writer.Write(@" }");
                    break;
            }
        }

        private static void WriteError(TextWriter writer, ErrorSchema schema, WriterMode mode, ISet<string> namedSchemas)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""name"":""");
                    writer.Write(schema.FullName);
                    writer.Write(@"""");
                    writer.Write(@",""type"":""error""");
                    writer.Write(@",""fields"":[");
                    WriteFields(writer, schema, mode, namedSchemas);
                    writer.Write(@"]");
                    writer.Write(@"}");
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ""type"": ""error""");
                    writer.Write(@", ""name"": """);
                    writer.Write(schema.Name);
                    writer.Write(@"""");
                    writer.Write(@", ""namespace"": """);
                    writer.Write(schema.Namespace);
                    writer.Write(@"""");
                    writer.Write(@", ""doc"": """);
                    writer.Write(schema.Doc);
                    writer.Write(@"""");
                    writer.Write(@", ""aliases"": [");
                    writer.Write(string.Join(", ", schema.Aliases.Select(r => $"\"{r}\"")));
                    writer.Write(@"]");
                    writer.Write(@", ""fields"": [");
                    WriteFields(writer, schema, mode, namedSchemas);
                    writer.Write(@"]");
                    writer.Write(@" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ""error""");
                    writer.Write(@", ""name"": """);
                    writer.Write(schema.Name);
                    writer.Write(@"""");
                    if (!string.IsNullOrEmpty(schema.Namespace))
                    {
                        writer.Write(@", ""namespace"": """);
                        writer.Write(schema.Namespace);
                        writer.Write(@"""");
                    }
                    if (!string.IsNullOrEmpty(schema.Doc))
                    {
                        writer.Write(@", ""doc"": """);
                        writer.Write(schema.Doc);
                        writer.Write(@"""");
                    }
                    if (schema.Aliases.Count > 0)
                    {
                        writer.Write(@", ""aliases"": [");
                        writer.Write(string.Join(", ", schema.Aliases.Select(r => $"\"{r}\"")));
                        writer.Write(@"]");
                    }
                    writer.Write(@", ""fields"": [");
                    WriteFields(writer, schema, mode, namedSchemas);
                    writer.Write(@"]");
                    writer.Write(@" }");
                    break;
            }
        }

        private static void WriteFields(TextWriter writer, IEnumerable<RecordSchema.Field> fields, WriterMode mode, ISet<string> namedSchemas)
        {
            var i = 0;
            foreach (var field in fields)
            {
                switch (mode)
                {
                    case WriterMode.Canonical:
                        if (i++ > 0)
                            writer.Write(",");
                        writer.Write("{");
                        writer.Write(@"""name"":""");
                        writer.Write(field.Name);
                        writer.Write(@"""");
                        writer.Write(@",""type"":");
                        Write(writer, field.Type, mode, namedSchemas);
                        if (field.Default != null)
                        {
                            writer.Write(@",""default"":""");
                            writer.Write(field.Default);
                            writer.Write(@"""");
                        }
                        writer.Write("}");
                        break;
                    case WriterMode.Full:
                        if (i++ > 0)
                            writer.Write(", ");
                        writer.Write("{");
                        writer.Write(@" ""name"": """);
                        writer.Write(field.Name);
                        writer.Write(@"""");
                        writer.Write(@", ""type"": ");
                        Write(writer, field.Type, mode, namedSchemas);
                        if (field.Default != null)
                        {
                            writer.Write(@", ""default"": """);
                            writer.Write(field.Default);
                            writer.Write(@"""");
                        }
                        writer.Write(@", ""doc"": """);
                        writer.Write(field.Doc);
                        writer.Write(@"""");
                        writer.Write(@", ""aliases"": [");
                        writer.Write(string.Join(", ", field.Aliases.Select(r => $"\"{r}\"")));
                        writer.Write(@"]");
                        writer.Write(@", ""order"": """);
                        writer.Write(field.Order ?? "ignore");
                        writer.Write(@"""");
                        writer.Write(@" }");
                        break;
                    default:
                        if (i++ > 0)
                            writer.Write(", ");
                        writer.Write("{");
                        writer.Write(@" ""name"": """);
                        writer.Write(field.Name);
                        writer.Write(@"""");
                        writer.Write(@", ""type"": ");
                        Write(writer, field.Type, mode, namedSchemas);
                        if (field.Default != null)
                        {
                            writer.Write(@", ""default"": """);
                            writer.Write(field.Default);
                            writer.Write(@"""");
                        }
                        if (!string.IsNullOrEmpty(field.Doc))
                        {
                            writer.Write(@", ""doc"": """);
                            writer.Write(field.Doc);
                            writer.Write(@"""");
                        }
                        if (field.Aliases.Count > 0)
                        {
                            writer.Write(@", ""aliases"": [");
                            writer.Write(string.Join(", ", field.Aliases.Select(r => $"\"{r}\"")));
                            writer.Write(@"]");
                        }
                        if (!string.IsNullOrEmpty(field.Order))
                        {
                            writer.Write(@", ""order"": """);
                            writer.Write(field.Order);
                            writer.Write(@"""");
                        }
                        writer.Write(@" }");
                        break;
                }
            }
        }

        private static void WriteLogicalType(TextWriter writer, LogicalSchema schema, WriterMode mode, ISet<string> namedSchemas)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""type"":");
                    Write(writer, schema.Type, mode, namedSchemas);
                    writer.Write(@",""logicalType"":""");
                    writer.Write(schema.LogicalType);
                    writer.Write(@"""}");
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ""type"": ");
                    Write(writer, schema.Type, mode, namedSchemas);
                    writer.Write(@", ""logicalType"": """);
                    writer.Write(schema.LogicalType);
                    writer.Write(@""" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ");
                    Write(writer, schema.Type, mode, namedSchemas);
                    writer.Write(@", ""logicalType"": """);
                    writer.Write(schema.LogicalType);
                    writer.Write(@""" }");
                    break;
            }
        }

        private static void WriteDecimal(TextWriter writer, DecimalSchema schema, WriterMode mode, ISet<string> namedSchemas)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""type"":");
                    Write(writer, schema.Type, mode, namedSchemas);
                    writer.Write(@",""logicalType"":""");
                    writer.Write(schema.LogicalType);
                    writer.Write(@"""");
                    writer.Write(@",""precision"":");
                    writer.Write(schema.Precision);
                    writer.Write(@",""scale"":");
                    writer.Write(schema.Scale);
                    writer.Write(@"}");
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ""type"": ");
                    Write(writer, schema.Type, mode, namedSchemas);
                    writer.Write(@", ""logicalType"": """);
                    writer.Write(schema.LogicalType);
                    writer.Write(@"""");
                    writer.Write(@", ""precision"": ");
                    writer.Write(schema.Precision);
                    writer.Write(@", ""scale"": ");
                    writer.Write(schema.Scale);
                    writer.Write(@" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ");
                    Write(writer, schema.Type, mode, namedSchemas);
                    writer.Write(@", ""logicalType"": """);
                    writer.Write(schema.LogicalType);
                    writer.Write(@"""");
                    writer.Write(@", ""precision"": ");
                    writer.Write(schema.Precision);
                    writer.Write(@", ""scale"": ");
                    writer.Write(schema.Scale);
                    writer.Write(@" }");
                    break;
            }
        }
    }
}
