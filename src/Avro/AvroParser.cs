using Avro.Protocol.Schema;
using Avro.Schema;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;

namespace Avro
{
    public static partial class AvroParser
    {
        public static AvroProtocol ReadProtocol(string text) => ReadProtocol(text, out _);

        public static AvroProtocol ReadProtocol(string text, out IEnumerable<NamedSchema> namedSchemas)
        {
            var namedTypes = new Dictionary<string, NamedSchema>();
            var jString = JSonEncodeString(text);

            var json = JToken.Parse(jString);
            var protocol = ParseProtocol(json, namedTypes, new Stack<string>(new string[] { string.Empty }));
            namedSchemas = namedTypes.Values;
            return protocol;
        }

        public static T ReadSchema<T>(string text) where T : AvroSchema => (T)ReadSchema(text);

        public static AvroSchema ReadSchema(string text) => ReadSchema(text, out _);

        public static AvroSchema ReadSchema(string text, out IEnumerable<NamedSchema> namedSchemas)
        {
            var namedTypes = new Dictionary<string, NamedSchema>();
            var jString = JSonEncodeString(text);

            var json = JToken.Parse(jString);
            var schema = ParseSchema(json, namedTypes, new Stack<string>(new string[] { string.Empty }));
            namedSchemas = namedTypes.Values;
            return schema;
        }

        private static string JSonEncodeString(string jString)
        {
            var trimmed = jString?.Trim() ?? string.Empty;
            if (trimmed.StartsWith('[') || trimmed.StartsWith('{') || trimmed.StartsWith('"') || double.TryParse(jString, out _))
                return trimmed;
            return $"\"{trimmed}\"";
        }

        private static string QualifyName(string name, Stack<string> enclosingNamespace)
        {
            if (name.Contains('.'))
                return name;
            if (!string.IsNullOrEmpty(enclosingNamespace.Peek()))
                return $"{enclosingNamespace.Peek()}.{name}";
            return name;
        }
    }
}
