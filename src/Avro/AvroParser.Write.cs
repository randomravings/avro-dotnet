using Avro.Protocol;
using Avro.Protocol.Schema;
using Avro.Schema;
using Avro.Utils;
using Newtonsoft.Json.Linq;
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
            Write(writer, schema, WriterMode.None, string.Empty, new HashSet<string>(), false);
        }

        public static void WriteAvroFull(TextWriter writer, AvroSchema schema)
        {
            Write(writer, schema, WriterMode.Full, string.Empty, new HashSet<string>(), false);
        }

        public static void WriteAvroCanonical(TextWriter writer, AvroSchema schema)
        {
            Write(writer, schema, WriterMode.Canonical, string.Empty, new HashSet<string>(), false);
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

        private static void Write(TextWriter writer, AvroSchema schema, WriterMode mode, string parentNamespace, ISet<string> namedSchemas, bool stripNs)
        {
            if (schema is EmptySchema)
                return;
            var ns = parentNamespace;
            if (schema is NamedSchema)
            {
                var name = ((NamedSchema)schema).FullName;
                if (stripNs && ((NamedSchema)schema).Namespace == parentNamespace)
                    name = ((NamedSchema)schema).Name;
                if (namedSchemas.Contains(((NamedSchema)schema).FullName))
                {
                    writer.Write(@"""");
                    writer.Write(name);
                    writer.Write(@"""");
                    return;
                }
                namedSchemas.Add(((NamedSchema)schema).FullName);
                ns = ((NamedSchema)schema).Namespace;
            }

            switch (schema)
            {
                case NullSchema _:
                case BooleanSchema _:
                case IntSchema _:
                case LongSchema _:
                case FloatSchema _:
                case DoubleSchema _:
                case BytesSchema _:
                case StringSchema _:
                    WritePrimitive(writer, schema, mode);
                    break;

                case ArraySchema s:
                    WriteArray(writer, s, mode, ns, namedSchemas, stripNs);
                    break;
                case MapSchema s:
                    WriteMap(writer, s, mode, ns, namedSchemas, stripNs);
                    break;

                case UnionSchema s:
                    WriteUnion(writer, s, mode, ns, namedSchemas, stripNs);
                    break;

                case FixedSchema s:
                    WriteFixed(writer, s, ns, mode, stripNs);
                    break;
                case EnumSchema s:
                    WriteEnum(writer, s, ns, mode, stripNs);
                    break;
                case ErrorSchema s:
                    WriteError(writer, s, mode, ns, namedSchemas, stripNs);
                    break;
                case RecordSchema s:
                    WriteRecord(writer, s, mode, ns, namedSchemas, stripNs);
                    break;

                case DecimalSchema s:
                    WriteDecimal(writer, s, mode, ns, namedSchemas, stripNs);
                    break;

                case LogicalSchema s:
                    WriteLogicalType(writer, s, mode, ns, namedSchemas, stripNs);
                    break;

                default:
                    throw new AvroException($"Unknown schema type: '{schema.GetType().FullName}'");
            }
        }

        private static void Write(TextWriter writer, AvroProtocol protocol, WriterMode mode)
        {
            var namedSchemas = new HashSet<string>();
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
                        WriteTypes(writer, protocol.Types, mode, protocol.Namespace, namedSchemas, true);
                        writer.Write(@"]");
                    }
                    if (protocol.Messages.Count() > 0)
                    {
                        writer.Write(@",""messages"":{");
                        WriteMessages(writer, protocol.Messages, mode, protocol.Namespace, namedSchemas, true);
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
                    WriteTypes(writer, protocol.Types, mode, protocol.Namespace, namedSchemas, true);
                    writer.Write(@"], ");
                    writer.Write(@"""messages"": {");
                    WriteMessages(writer, protocol.Messages, mode, protocol.Namespace, namedSchemas, true);
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
                        WriteTypes(writer, protocol.Types, mode, protocol.Namespace, namedSchemas, true);
                        writer.Write(@"]");
                    }
                    if (protocol.Messages.Count() > 0)
                    {
                        writer.Write(@", ""messages"": {");
                        WriteMessages(writer, protocol.Messages, mode, protocol.Namespace, namedSchemas, true);
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

        private static void WriteArray(TextWriter writer, ArraySchema schema, WriterMode mode, string parentNamespace, ISet<string> namedSchemas, bool stripNs)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""type"":""array"",""items"":");
                    Write(writer, schema.Items, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@"}");
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ""type"": ""array"", ""items"": ");
                    Write(writer, schema.Items, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ""array"", ""items"": ");
                    Write(writer, schema.Items, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@" }");
                    break;
            }
        }

        private static void WriteMap(TextWriter writer, MapSchema schema, WriterMode mode, string parentNamespace, ISet<string> namedSchemas, bool stripNs)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""type"":""map"",""values"":");
                    Write(writer, schema.Values, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@"}");
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ""type"": ""map"", ""values"": ");
                    Write(writer, schema.Values, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ""map"", ""values"": ");
                    Write(writer, schema.Values, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@" }");
                    break;
            }
        }

        private static void WriteUnion(TextWriter writer, UnionSchema schema, WriterMode mode, string parentNamespace, ISet<string> namedSchemas, bool stripNs)
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
                        Write(writer, item, mode, parentNamespace, namedSchemas, stripNs);
                    }
                    writer.Write(@"]");
                    break;
                case WriterMode.Full:
                    writer.Write(@"[");
                    foreach (var item in schema)
                    {
                        if (i++ > 0)
                            writer.Write(", ");
                        Write(writer, item, mode, parentNamespace, namedSchemas, stripNs);
                    }
                    writer.Write(@"]");
                    break;
                default:
                    writer.Write(@"[");
                    foreach (var item in schema)
                    {
                        if (i++ > 0)
                            writer.Write(", ");
                        Write(writer, item, mode, parentNamespace, namedSchemas, stripNs);
                    }
                    writer.Write(@"]");
                    break;
            }
        }

        private static void WriteFixed(TextWriter writer, FixedSchema schema, string parentNamespace, WriterMode mode, bool stripNs)
        {
            var ns = parentNamespace;
            if (!string.IsNullOrEmpty(schema.Namespace))
                ns = schema.Namespace;
            if (stripNs && ns == parentNamespace)
                ns = string.Empty;
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
                    if (!string.IsNullOrEmpty(ns))
                    {
                        writer.Write(@", ""namespace"": """);
                        writer.Write(ns);
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

        private static void WriteEnum(TextWriter writer, EnumSchema schema, string parentNamespace, WriterMode mode, bool stripNs)
        {
            var ns = parentNamespace;
            if (!string.IsNullOrEmpty(schema.Namespace))
                ns = schema.Namespace;
            if (stripNs && ns == parentNamespace)
                ns = string.Empty;
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""name"":""");
                    writer.Write(schema.FullName);
                    writer.Write(@"""");
                    writer.Write(@",""type"":""enum""");
                    writer.Write(@",""symbols"":[");
                    writer.Write(string.Join(",", schema.Keys.Select(r => $"\"{r}\"")));
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
                    writer.Write(string.Join(", ", schema.Keys.Select(r => $"\"{r}\"")));
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
                    if (!string.IsNullOrEmpty(ns))
                    {
                        writer.Write(@", ""namespace"": """);
                        writer.Write(ns);
                        writer.Write(@"""");
                    }
                    writer.Write(@", ""symbols"": [");
                    writer.Write(string.Join(", ", schema.Keys.Select(r => $"\"{r}\"")));
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


        private static void WriteRecord(TextWriter writer, RecordSchema schema, WriterMode mode, string parentNamespace, ISet<string> namedSchemas, bool stripNs)
        {
            var ns = parentNamespace;
            if (!string.IsNullOrEmpty(schema.Namespace))
                ns = schema.Namespace;
            if (stripNs && ns == parentNamespace)
                ns = string.Empty;
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""name"":""");
                    writer.Write(schema.FullName);
                    writer.Write(@"""");
                    writer.Write(@",""type"":""record""");
                    writer.Write(@",""fields"":[");
                    WriteFields(writer, schema, mode, parentNamespace, namedSchemas, stripNs);
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
                    WriteFields(writer, schema, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@"]");
                    writer.Write(@" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ""record""");
                    writer.Write(@", ""name"": """);
                    writer.Write(schema.Name);
                    writer.Write(@"""");
                    if (!string.IsNullOrEmpty(ns))
                    {
                        writer.Write(@", ""namespace"": """);
                        writer.Write(ns);
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
                    WriteFields(writer, schema, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@"]");
                    writer.Write(@" }");
                    break;
            }
        }

        private static void WriteError(TextWriter writer, ErrorSchema schema, WriterMode mode, string parentNamespace, ISet<string> namedSchemas, bool stripNs)
        {
            var ns = parentNamespace;
            if (!string.IsNullOrEmpty(schema.Namespace))
                ns = schema.Namespace;
            if (stripNs && ns == parentNamespace)
                ns = string.Empty;
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""name"":""");
                    writer.Write(schema.FullName);
                    writer.Write(@"""");
                    writer.Write(@",""type"":""error""");
                    writer.Write(@",""fields"":[");
                    WriteFields(writer, schema, mode, parentNamespace, namedSchemas, stripNs);
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
                    WriteFields(writer, schema, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@"]");
                    writer.Write(@" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ""error""");
                    writer.Write(@", ""name"": """);
                    writer.Write(schema.Name);
                    writer.Write(@"""");
                    if (!string.IsNullOrEmpty(ns))
                    {
                        writer.Write(@", ""namespace"": """);
                        writer.Write(ns);
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
                    WriteFields(writer, schema, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@"]");
                    writer.Write(@" }");
                    break;
            }
        }

        private static void WriteFields(TextWriter writer, IEnumerable<FieldSchema> fields, WriterMode mode, string parentNamespace, ISet<string> namedSchemas, bool stripNs)
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
                        Write(writer, field.Type, mode, parentNamespace, namedSchemas, stripNs);
                        if (!field.Default.Equals(JsonUtil.EmptyDefault))
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
                        Write(writer, field.Type, mode, parentNamespace, namedSchemas, stripNs);
                        writer.Write(@", ""default"": ");
                        writer.Write(field.Default);
                        writer.Write(@"");
                        writer.Write(@", ""doc"": """);
                        writer.Write(field.Doc);
                        writer.Write(@"""");
                        writer.Write(@", ""aliases"": [");
                        writer.Write(string.Join(", ", field.Aliases.Select(r => $"\"{r}\"")));
                        writer.Write(@"]");
                        writer.Write(@", ""order"": """);
                        writer.Write(field.Order);
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
                        Write(writer, field.Type, mode, parentNamespace, namedSchemas, stripNs);
                        if (!field.Default.Equals(JsonUtil.EmptyDefault))
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
                        if (field.Order != "ignore")
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

        private static void WriteLogicalType(TextWriter writer, LogicalSchema schema, WriterMode mode, string parentNamespace, ISet<string> namedSchemas, bool stripNs)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""type"":");
                    Write(writer, schema.Type, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@",""logicalType"":""");
                    writer.Write(schema.LogicalType);
                    writer.Write(@"""}");
                    break;
                case WriterMode.Full:
                    writer.Write(@"{ ""type"": ");
                    Write(writer, schema.Type, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@", ""logicalType"": """);
                    writer.Write(schema.LogicalType);
                    writer.Write(@""" }");
                    break;
                default:
                    writer.Write(@"{ ""type"": ");
                    Write(writer, schema.Type, mode, parentNamespace, namedSchemas, stripNs);
                    writer.Write(@", ""logicalType"": """);
                    writer.Write(schema.LogicalType);
                    writer.Write(@""" }");
                    break;
            }
        }

        private static void WriteDecimal(TextWriter writer, DecimalSchema schema, WriterMode mode, string parentNamespace, ISet<string> namedSchemas, bool stripNs)
        {
            switch (mode)
            {
                case WriterMode.Canonical:
                    writer.Write(@"{""type"":");
                    Write(writer, schema.Type, mode, parentNamespace, namedSchemas, stripNs);
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
                    Write(writer, schema.Type, mode, parentNamespace, namedSchemas, stripNs);
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
                    Write(writer, schema.Type, mode, parentNamespace, namedSchemas, stripNs);
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

        private static void WriteTypes(TextWriter writer, IEnumerable<AvroSchema> schemas, WriterMode mode, string parentNamespace, ISet<string> namedSchemas, bool stripNs)
        {
            var i = 0;
            foreach (var schema in schemas)
            {
                switch (mode)
                {
                    case WriterMode.Canonical:
                        if (i > 0)
                            writer.Write(",");
                        Write(writer, schema, mode, parentNamespace, namedSchemas, stripNs);
                        break;
                    case WriterMode.Full:
                        if (i > 0)
                            writer.Write(", ");
                        Write(writer, schema, mode, parentNamespace, namedSchemas, stripNs);
                        break;
                    default:
                        if (i > 0)
                            writer.Write(", ");
                        Write(writer, schema, mode, parentNamespace, namedSchemas, stripNs);
                        break;
                }
                i++;
            }
        }

        private static void WriteMessages(TextWriter writer, IEnumerable<MessageSchema> messages, WriterMode mode, string parentNamespace, ISet<string> namedSchemas, bool stripNs)
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
                        WriteParameters(writer, message.RequestParameters, mode, parentNamespace, stripNs);
                        writer.Write(@"]");
                        if (!(message.Response is EmptySchema))
                        {
                            writer.Write(@",""response"":");
                            Write(writer, message.Response ?? EmptySchema.Value, mode, parentNamespace, namedSchemas, stripNs);
                        }
                        if (message.Error.Count > 1)
                        {
                            writer.Write(@",""errors"":[");
                            writer.Write(string.Join(",", message.Error.Skip(1).Select(r => $"\"{((NamedSchema)r).FullName}\"")));
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
                        WriteParameters(writer, message.RequestParameters, mode, parentNamespace, stripNs);
                        writer.Write(@"], ");
                        writer.Write(@"""response"": ");
                        if (!(message.Response is EmptySchema))
                            Write(writer, message.Response ?? EmptySchema.Value, mode, parentNamespace, namedSchemas, stripNs);
                        else
                            writer.Write("null");
                        writer.Write(@", ""errors"": [");
                        writer.Write(string.Join(", ", message.Error.Skip(1).Select(r => $"\"{((NamedSchema)r).FullName}\"")));
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
                        WriteParameters(writer, message.RequestParameters, mode, parentNamespace, stripNs);
                        writer.Write(@"]");
                        if (!(message.Response is EmptySchema))
                        {
                            writer.Write(@", ""response"": ");
                            Write(writer, message.Response ?? EmptySchema.Value, mode, parentNamespace, namedSchemas, stripNs);
                        }
                        if (message.Error.Count > 1)
                        {
                            writer.Write(@", ""errors"": [");
                            writer.Write(string.Join(", ", message.Error.Skip(1).Cast<NamedSchema>().Select(r => $"\"{(stripNs && r.Namespace == parentNamespace ? r.Name : r.FullName)}\"")));
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

        private static void WriteParameters(TextWriter writer, IEnumerable<ParameterSchema> requestParameters, WriterMode mode, string parentNamespace, bool stripNs)
        {
            var i = 0;
            foreach (var requestParameter in requestParameters)
            {
                var ns = parentNamespace;
                if (!string.IsNullOrEmpty(requestParameter.Type.Namespace))
                    ns = requestParameter.Type.Namespace;
                if (stripNs && ns == parentNamespace)
                    ns = string.Empty;
                var name = ns == string.Empty ? requestParameter.Type.Name : $"{ns}.{requestParameter.Type.Name}";
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
                        writer.Write(requestParameter.Type.FullName);
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
                        writer.Write(requestParameter.Type.FullName);
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
                        writer.Write(name);
                        writer.Write(@""" }");
                        break;
                }
            }
        }
    }
}
