using Avro.Protocol;
using Avro.Protocol.Schema;
using Avro.Schema;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Avro
{
    public static partial class AvroParser
    {
        private enum WriterMode
        {
            None,
            Canonical,
            Full
        };

        public static void WriteAvro(TextWriter writer, AvroSchema schema)
        {
            Write(writer, schema, WriterMode.None);
        }

        public static void WriteAvroFull(TextWriter writer, AvroSchema schema)
        {
            Write(writer, schema, WriterMode.Full);
        }

        public static void WriteAvroCanonical(TextWriter writer, AvroSchema schema)
        {
            Write(writer, schema, WriterMode.Canonical);
        }

        public static string ToAvroCanonical(this AvroSchema schema)
        {
            var sb = new StringBuilder();
            using (var sw = new StringWriter(sb))
                WriteAvroCanonical(sw, schema);
            return sb.ToString();
        }

        public static string ToAvro(this AvroSchema schema)
        {
            var sb = new StringBuilder();
            using (var sw = new StringWriter(sb))
                WriteAvro(sw, schema);
            return sb.ToString();
        }

        public static string ToAvroFull(this AvroSchema schema)
        {
            var sb = new StringBuilder();
            using (var sw = new StringWriter(sb))
                WriteAvroFull(sw, schema);
            return sb.ToString();
        }

        public static string ToAvroCanonical(this AvroProtocol protocol)
        {
            var sb = new StringBuilder();
            using (var sw = new StringWriter(sb))
                WriteAvroCanonical(sw, protocol);
            return sb.ToString();
        }

        public static string ToAvro(this AvroProtocol protocol)
        {
            var sb = new StringBuilder();
            using (var sw = new StringWriter(sb))
                WriteAvro(sw, protocol);
            return sb.ToString();
        }

        public static string ToAvroFull(this AvroProtocol protocol)
        {
            var sb = new StringBuilder();
            using (var sw = new StringWriter(sb))
                WriteAvroFull(sw, protocol);
            return sb.ToString();
        }

        public static void WriteAvro(TextWriter writer, AvroProtocol protocol)
        {
            Write(writer, protocol, WriterMode.None);
        }

        public static void WriteAvroFull(TextWriter writer, AvroProtocol protocol)
        {
            Write(writer, protocol, WriterMode.Full);
        }

        public static void WriteAvroCanonical(TextWriter writer, AvroProtocol protocol)
        {
            Write(writer, protocol, WriterMode.Canonical);
        }

        private static void Write(TextWriter writer, AvroSchema schema, WriterMode mode, ISet<string> namedSchemas = null)
        {
            if (namedSchemas == null)
                namedSchemas = new HashSet<string>();
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
                    if (schema is LogicalSchema)
                        WriteLogicalType(writer, schema as LogicalSchema, mode, namedSchemas);
                    else
                        throw new AvroException($"Unknown schema type: '{schema.GetType().FullName}'");
                    break;
            }
        }

        private static void Write(TextWriter writer, AvroProtocol protocol, WriterMode mode, ISet<string> namedSchemas = null)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{");
                    writer.Write(@"""protocol"":""");
                    writer.Write(protocol.FullName);
                    writer.Write(@"""");
                    if (protocol.Types.Count() > 0)
                    {
                        writer.Write(@",""types"":[");
                        WriteTypes(writer, protocol.Types, mode, namedSchemas);
                        writer.Write(@"]");
                    }
                    if (protocol.Messages.Count() > 0)
                    {
                        writer.Write(@",""messages"":{");
                        WriteMessages(writer, protocol.Messages, mode, namedSchemas);
                        writer.Write(@"}");
                    }
                    writer.Write("}"); ;
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ");
                    writer.Write(@"""namespace"": """);
                    writer.Write(protocol.Namespace);
                    writer.Write(@""", ");
                    writer.Write(@"""protocol"": """);
                    writer.Write(protocol.Name);
                    writer.Write(@""", ");
                    writer.Write(@"""doc"": """);
                    writer.Write(protocol.Doc);
                    writer.Write(@""", ");
                    writer.Write(@"""types"": [");
                    WriteTypes(writer, protocol.Types, mode, namedSchemas);
                    writer.Write(@"], ");
                    writer.Write(@"""messages"": {");
                    WriteMessages(writer, protocol.Messages, mode, namedSchemas);
                    writer.Write("}"); ;
                    writer.Write(" }"); ;
                    break;
                default:
                    writer.Write(@"{ ");
                    if (!string.IsNullOrEmpty(protocol.Namespace))
                    {
                        writer.Write(@"""namespace"": """);
                        writer.Write(protocol.Namespace);
                        writer.Write(@""", ");
                    }
                    writer.Write(@"""protocol"": """);
                    writer.Write(protocol.Name);
                    writer.Write(@"""");
                    if (!string.IsNullOrEmpty(protocol.Doc))
                    {
                        writer.Write(@", ""doc"": """);
                        writer.Write(protocol.Doc);
                        writer.Write(@"""");
                    }
                    if (protocol.Types.Count() > 0)
                    {
                        writer.Write(@", ""types"": [");
                        WriteTypes(writer, protocol.Types, mode, namedSchemas);
                        writer.Write(@"]");
                    }
                    if (protocol.Messages.Count() > 0)
                    {
                        writer.Write(@", ""messages"": {");
                        WriteMessages(writer, protocol.Messages, mode, namedSchemas);
                        writer.Write(@"}");
                    }
                    writer.Write(" }");
                    break;
            }
        }

        private static void WritePrimitive(TextWriter writer, AvroSchema schema, WriterMode mode)
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
                            writer.Write(@",""default"":");
                            writer.Write(field.Default);
                            writer.Write(@"");
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
                            writer.Write(@", ""default"": ");
                            writer.Write(field.Default);
                            writer.Write(@"");
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
                            writer.Write(@", ""default"": ");
                            writer.Write(field.Default);
                            writer.Write(@"");
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

        private static void WriteTypes(TextWriter writer, IEnumerable<AvroSchema> schemas, WriterMode mode, ISet<string> namedSchemas)
        {
            var i = 0;
            foreach (var schema in schemas)
            {
                switch (mode)
                {
                    case WriterMode.Canonical:
                        if (i > 0)
                            writer.Write(",");
                        Write(writer, schema, mode, namedSchemas);
                        break;
                    case WriterMode.Full:
                        if (i > 0)
                            writer.Write(", ");
                        Write(writer, schema, mode, namedSchemas);
                        break;
                    default:
                        if (i > 0)
                            writer.Write(", ");
                        Write(writer, schema, mode, namedSchemas);
                        break;
                }
                i++;
            }
        }

        private static void WriteMessages(TextWriter writer, IEnumerable<MessageSchema> messages, WriterMode mode, ISet<string> namedSchemas)
        {
            var i = 0;
            foreach (var message in messages)
            {
                switch (mode)
                {
                    case WriterMode.Canonical:
                        if (i > 0)
                            writer.Write(",");
                        writer.Write(@"""");
                        writer.Write(message.Name);
                        writer.Write(@""":");
                        writer.Write(@"{");
                        writer.Write(@"""request"":[");
                        WriteParameters(writer, message.RequestParameters, mode, namedSchemas);
                        writer.Write(@"]");
                        if (message.Response != null)
                        {
                            writer.Write(@",""response"":");
                            Write(writer, message.Response, mode, namedSchemas);
                        }
                        if (message.Error.Count > 1)
                        {
                            writer.Write(@",""errors"":[");
                                writer.Write(string.Join(",", message.Error.Skip(1).Select(r => $"\"{(r as NamedSchema).Name}\"")));
                            writer.Write(@"]");
                        }
                        if (message.Oneway)
                        {
                            writer.Write(@",""one-way"":true");
                        }
                        writer.Write(@"}");
                        break;
                    case WriterMode.Full:
                        if (i > 0)
                            writer.Write(", ");
                        writer.Write(@"""");
                        writer.Write(message.Name);
                        writer.Write(@""": ");
                        writer.Write(@"{");
                        writer.Write(@"""doc"": """);
                        writer.Write(message.Doc);
                        writer.Write(@"""");
                        writer.Write(@", ""request"": [");
                        WriteParameters(writer,  message.RequestParameters, mode, namedSchemas);
                        writer.Write(@"], ");
                        writer.Write(@"""response"": ");
                        if (message.Response != null)
                            Write(writer, message.Response, mode, namedSchemas);
                        else
                            writer.Write("null");
                        writer.Write(@", ""errors"": [");
                        writer.Write(string.Join(", ", message.Error.Skip(1).Select(r => $"\"{(r as NamedSchema).Name}\"")));
                        writer.Write(@"], ");
                        writer.Write(@"""one-way"": ");
                        writer.Write(message.Oneway.ToString().ToLower());
                        writer.Write(@"}");
                        break;
                    default:
                        if (i > 0)
                            writer.Write(", ");
                        writer.Write(@"""");
                        writer.Write(message.Name);
                        writer.Write(@""": ");
                        writer.Write(@"{");
                        if (!string.IsNullOrEmpty(message.Doc))
                        {
                            writer.Write(@"""doc"": """);
                            writer.Write(message.Doc);
                            writer.Write(@""", ");
                        }
                        writer.Write(@"""request"": [");
                        WriteParameters(writer, message.RequestParameters, mode, namedSchemas);
                        writer.Write(@"]");
                        if (message.Response != null)
                        {
                            writer.Write(@", ""response"": ");
                            Write(writer, message.Response, mode, namedSchemas);
                        }
                        if (message.Error.Count > 1)
                        {
                            writer.Write(@", ""errors"": [");
                            writer.Write(string.Join(", ", message.Error.Skip(1).Select(r => $"\"{(r as NamedSchema).Name}\"")));
                            writer.Write(@"]");
                        }
                        if (message.Oneway)
                        {
                            writer.Write(@", ""one-way"": true");
                        }
                        writer.Write(@"}");
                        break;
                }
                i++;
            }
        }

        private static void WriteParameters(TextWriter writer, IEnumerable<ParameterSchema> requestParameters, WriterMode mode, ISet<string> namedSchemas)
        {
            var i = 0;
            foreach (var requestParameter in requestParameters)
            {
                switch (mode)
                {
                    case WriterMode.Canonical:
                        if (i++ > 0)
                            writer.Write(",");
                        writer.Write("{");
                        writer.Write(@"""name"":""");
                        writer.Write(requestParameter.Name);
                        writer.Write(@"""");
                        writer.Write(@",""type"":""");
                        writer.Write(requestParameter.Type);
                        writer.Write(@"""}");
                        break;
                    case WriterMode.Full:
                        if (i++ > 0)
                            writer.Write(", ");
                        writer.Write("{");
                        writer.Write(@" ""name"": """);
                        writer.Write(requestParameter.Name);
                        writer.Write(@"""");
                        writer.Write(@", ""type"": """);
                        writer.Write(requestParameter.Type);
                        writer.Write(@""" }");
                        break;
                    default:
                        if (i++ > 0)
                            writer.Write(", ");
                        writer.Write("{");
                        writer.Write(@" ""name"": """);
                        writer.Write(requestParameter.Name);
                        writer.Write(@"""");
                        writer.Write(@", ""type"": """);
                        writer.Write(requestParameter.Type);
                        writer.Write(@""" }");
                        break;
                }
            }
        }
    }
}
