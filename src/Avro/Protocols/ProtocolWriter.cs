using Avro.Protocols;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Avro.Schemas
{
    public static class ProtocolWriter
    {
        public static void Write(Protocol protocol, TextWriter writer, ISet<string> namedSchemas = null)
        {
            if (namedSchemas == null)
                namedSchemas = new HashSet<string>();
            Write(protocol, WriterMode.None, writer, namedSchemas);
        }

        public static void WriteFull(Protocol protocol, TextWriter writer, ISet<string> namedSchemas)
        {
            if (namedSchemas == null)
                namedSchemas = new HashSet<string>();
            Write(protocol, WriterMode.Full, writer, namedSchemas);
        }

        public static void WriteCanonical(Protocol protocol, TextWriter writer, ISet<string> namedSchemas)
        {
            if (namedSchemas == null)
                namedSchemas = new HashSet<string>();
            Write(protocol, WriterMode.Canonical, writer, namedSchemas);
        }

        private static void Write(Protocol protocol, WriterMode mode, TextWriter writer, ISet<string> namedSchemas)
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
                        writer.Write(@", ""types"":");
                        WriteTypes(protocol.Types, mode, writer, namedSchemas);
                    }
                    if (protocol.Messages.Count() > 0)
                    {
                        writer.Write(@", ""messages"":");
                        WriteMessages(protocol.Messages, mode, writer, namedSchemas);
                    }
                    writer.Write("}"); ;
                    break;
                case WriterMode.Full:
                    writer.Write(@"{");
                    writer.Write(@"""protocol"":""");
                    writer.Write(protocol.FullName);
                    writer.Write(@"""");
                    writer.Write(@", ""types"": [");
                    WriteTypes(protocol.Types, mode, writer, namedSchemas);
                    writer.Write(@"]");
                    writer.Write(@", ""messages"": {");
                    WriteMessages(protocol.Messages, mode, writer, namedSchemas);
                    writer.Write("}"); ;
                    writer.Write("}"); ;
                    break;
                default:
                    writer.Write(@"{");
                    writer.Write(@"""protocol"":""");
                    writer.Write(protocol.FullName);
                    writer.Write(@"""");
                    if (protocol.Types.Count() > 0)
                    {
                        writer.Write(@", ""types"":");
                        WriteTypes(protocol.Types, mode, writer, namedSchemas);
                    }
                    if (protocol.Messages.Count() > 0)
                    {
                        writer.Write(@", ""messages"":");
                        WriteMessages(protocol.Messages, mode, writer, namedSchemas);
                    }
                    writer.Write("}");
                    break;
            }
        }

        private static void WriteTypes(IEnumerable<Schema> schemas, WriterMode mode, TextWriter writer, ISet<string> namedSchemas)
        {
            var i = 0;
            foreach (var schema in schemas)
            {
                switch (mode)
                {
                    case WriterMode.Canonical:
                        if (i > 0)
                            writer.Write(",");
                        SchemaWriter.WriteCanonical(writer, schema, namedSchemas);
                        break;
                    case WriterMode.Full:
                        if (i > 0)
                            writer.Write(", ");
                        SchemaWriter.WriteFull(writer, schema, namedSchemas);
                        break;
                    default:
                        if (i > 0)
                            writer.Write(", ");
                        SchemaWriter.Write(writer, schema, namedSchemas);
                        break;
                }
            }
            writer.Write(@", ""types"":[{string.Join(", ", Types.Select(r => r.ToAvroString(namedSchemas)))}]");
        }

        private static void WriteMessages(IEnumerable<Message> messages, WriterMode mode, TextWriter writer, ISet<string> namedSchemas)
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
                        writer.Write(@"""doc"":""");
                        writer.Write(message.Doc);
                        writer.Write(@"""");
                        writer.Write(@"""request"":[");
                        writer.Write(message.Request);
                        writer.Write(@"]");
                        writer.Write(@"""response"":[");
                        SchemaWriter.Write(writer, message.Response, namedSchemas);
                        writer.Write(@"]");
                        writer.Write(@"""error"":[");
                        SchemaWriter.Write(writer, message.Response, namedSchemas);
                        writer.Write(@"]");
                        break;
                    case WriterMode.Full:
                        if (i > 0)
                            writer.Write(",");
                        break;
                    default:
                        if (i > 0)
                            writer.Write(",");
                        writer.Write(@"""");
                        writer.Write(message.Name);
                        writer.Write(@""":");
                        writer.Write(@"{");
                        writer.Write(@"""doc"":""");
                        writer.Write(message.Doc);
                        writer.Write(@"""");
                        writer.Write(@"""request"":[");
                        writer.Write(message.Request);
                        writer.Write(@"]");
                        writer.Write(@"""response"":[");
                        SchemaWriter.Write(writer, message.Response, namedSchemas);
                        writer.Write(@"]");
                        writer.Write(@"""error"":[");
                        SchemaWriter.Write(writer, message.Response, namedSchemas);
                        writer.Write(@"]");
                        break;
                }
            }
        }
    }
}
