using Avro.Schemas;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace Avro.Utils
{
    public static class DefaultValidator
    {
        public static void ValidateString(Schema schema, string value)
        {
            var jToken = default(JToken);
            if (value != null)
                jToken = JToken.Parse(value);
            ValidateJson(schema, jToken);
        }

        public static void ValidateJson(Schema schema, JToken jToken)
        {
            if (jToken == null)
                return;

            switch (schema)
            {
                case NullSchema n when jToken.Type == JTokenType.Null:
                    break;
                case BooleanSchema b when jToken.Type == JTokenType.Boolean:
                    break;
                case IntSchema i when jToken.Type == JTokenType.Integer &&
                        int.TryParse(jToken.ToString(), out _):
                    break;
                case LongSchema l when jToken.Type == JTokenType.Integer &&
                        long.TryParse(jToken.ToString(), out _):
                    break;
                case FloatSchema f when (jToken.Type == JTokenType.Integer || jToken.Type == JTokenType.Float) &&
                        float.TryParse(jToken.ToString(), out _):
                    break;
                case DoubleSchema d when (jToken.Type == JTokenType.Integer || jToken.Type == JTokenType.Float) &&
                        double.TryParse(jToken.ToString(), out _):
                    break;
                case BytesSchema b when jToken.Type == JTokenType.String &&
                        Regex.IsMatch(jToken.ToString(), @"^(\\u00[0-9a-fA-F][0-9a-fA-F])*$"):
                    break;
                case StringSchema s when jToken.Type == JTokenType.String:
                    break;
                case ArraySchema s when jToken.Type == JTokenType.Array:
                    foreach (var item in jToken as JArray)
                        ValidateJson(s.Items, item);
                    break;
                case MapSchema s when jToken.Type == JTokenType.Object:
                    foreach (var item in jToken as JObject)
                        ValidateJson(s.Values, item.Value);
                    break;
                case EnumSchema e when jToken.Type == JTokenType.String &&
                        Regex.IsMatch(jToken.ToString().Trim('"'), @"^[a-zA-Z][0-9a-zA-Z]*$") &&
                        e.Symbols.Contains(jToken.ToString().Trim('"')):
                    break;
                case FixedSchema f when jToken.Type == JTokenType.String &&
                        Regex.Match(jToken.ToString(), @"^(\\u00[0-9a-fA-F][0-9a-fA-F])*$").Length == f.Size * 6:
                    break;
                case RecordSchema r when jToken.Type == JTokenType.Object &&
                    !r.Any(f => f.Default == null && !(jToken as JObject).ContainsKey(f.Name)) && // No fields without defaults omitted in default value.
                    !(jToken as JObject).Properties().Any(k => r.FirstOrDefault(f => f.Name == k.Name) == null): // No invalud default values references
                    foreach (var defaultValue in (jToken as JObject))
                        ValidateJson(r.First(f => f.Name == defaultValue.Key).Type, defaultValue.Value);
                    break;
                case UnionSchema u:
                    ValidateJson(u[0], jToken);
                    break;
                case UuidSchema u:
                    if (jToken.Type != JTokenType.String || !Guid.TryParse(jToken.ToString().Trim('"'), out _))
                        throw new AvroParseException($"Invalid default value for schema '{schema.GetType().Name}': '{jToken.ToString()}'");
                    break;
                case LogicalSchema l:
                    ValidateJson(l.Type, jToken);
                    break;
                default:
                    throw new AvroParseException($"Invalid default value for schema '{schema.GetType().Name}': '{jToken.ToString()}'");
            }
        }
    }
}
