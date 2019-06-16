using Avro.Utils;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Schemas
{
    internal static class SchemaParser
    {
        internal static Schema Parse(string text)
        {
            var namedTypes = new Dictionary<string, NamedSchema>();
            var jString = JSonEncodeString(text);
            var json = JToken.Parse(jString);
            try
            {
                return Parse(json, namedTypes);
            }
            catch (ArgumentException ex)
            {
                throw new SchemaParseException("Error during schema parsing", ex);
            }
        }

        internal static Schema Parse(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            switch (jToken.Type)
            {
                case JTokenType.Array:
                    return ParseUnionSchema(jToken, namedTypes);
                case JTokenType.Object:
                    return ParseComplexSchema(jToken, namedTypes);
                case JTokenType.String:
                    return ParsePrimitiveSchema(jToken.ToString(), namedTypes);
                default:
                    throw new SchemaParseException($"Unexpected Json token: '{jToken.Type}'");
            }
        }

        private static Schema ParseUnionSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var jArray = jToken as JArray;
            if (jArray.Count == 1)
                return Parse(jArray.First(), namedTypes);
            var unionSchema = new UnionSchema();
            foreach (var item in jArray)
            {
                var schema = Parse(item, namedTypes);
                unionSchema.Add(schema);
            }
            return unionSchema;
        }

        private static Schema ParseComplexSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var type = JsonUtil.GetValue<JToken>(jToken, "type");

            if (JsonUtil.TryGetValue<JToken>(jToken, "logicalType", out _))
                return ParseLogicalSchema(jToken, namedTypes);

            if (type.Type == JTokenType.Array)
                return ParseUnionSchema(type as JArray, namedTypes);

            switch (type.ToString())
            {
                case "array":
                    return ParseArraySchema(jToken, namedTypes);
                case "map":
                    return ParseMapSchema(jToken, namedTypes);
                case "fixed":
                    return ParseFixedSchema(jToken, namedTypes);
                case "enum":
                    return ParseEnumType(jToken, namedTypes);
                case "record":
                case "error":
                    return ParseRecordSchema(jToken, namedTypes);
                default:
                    return ParsePrimitiveSchema(type, namedTypes);
            }
        }

        private static Schema ParseLogicalSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var logicalType = JsonUtil.GetValue<string>(jToken, "logicalType");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");

            switch (logicalType)
            {
                case "decimal":
                    return ParseDecimalSchema(jToken, namedTypes);
                case "time-millis":
                    return ParseTimeMillisSchema(jToken, namedTypes);
                case "time-micros":
                    return ParseTimeMicrosSchema(jToken, namedTypes);
                case "time-nanos":
                    return ParseTimeNanosSchema(jToken, namedTypes);
                case "timestamp-millis":
                    return ParseTimestampMillisSchema(jToken, namedTypes);
                case "timestamp-micros":
                    return ParseTimestampMicrosSchema(jToken, namedTypes);
                case "timestamp-nanos":
                    return ParseTimestampNanosSchema(jToken, namedTypes);
                case "duration":
                    return ParseDurationSchema(jToken, namedTypes);
                case "uuid":
                    return ParseUuidSchema(jToken, namedTypes);
                default:
                    return Parse(type, namedTypes);
            }
        }

        private static Schema ParseArraySchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "type", "items" };
            JsonUtil.AssertKeys(jToken, keys);

            JsonUtil.AssertValue(jToken, "type", "array");
            var items = JsonUtil.GetValue<JToken>(jToken, "items");
            var itemsSchema = Parse(items, namedTypes);
            return new ArraySchema(itemsSchema);
        }

        private static Schema ParseMapSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "type", "values" };
            JsonUtil.AssertKeys(jToken, keys);

            JsonUtil.AssertValue(jToken, "type", "map");
            var values = JsonUtil.GetValue<JToken>(jToken, "values");
            var valuesSchema = Parse(values, namedTypes);
            return new MapSchema(valuesSchema);
        }

        private static Schema ParseFixedSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "type", "size", "name" };
            var optionalKeys = new HashSet<string>() { "namespace" };
            JsonUtil.AssertKeys(jToken, keys, optionalKeys);

            JsonUtil.AssertValue(jToken, "type", "fixed");
            var size = JsonUtil.GetValue<int>(jToken, "size");
            var name = JsonUtil.GetValue<string>(jToken, "name");
            JsonUtil.TryGetValue<string>(jToken, "namespace", out var ns);
            var fixedSchema = new FixedSchema(name, ns, size);
            namedTypes.Add(fixedSchema.FullName, fixedSchema);
            return fixedSchema;
        }

        private static Schema ParseDecimalSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "logicalType", "type", "precision", "scale" };
            JsonUtil.AssertKeys(jToken, keys);

            JsonUtil.AssertValue(jToken, "logicalType", "decimal");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var precision = JsonUtil.GetValue<int>(jToken, "precision");
            var scale = JsonUtil.GetValue<int>(jToken, "scale");
            var underlyingType = Parse(type, namedTypes);
            return new DecimalSchema(underlyingType, precision, scale);
        }

        private static Schema ParseUuidSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys);

            JsonUtil.AssertValue(jToken, "logicalType", "uuid");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = Parse(type, namedTypes);
            return new UuidSchema(underlyingType);
        }

        private static Schema ParseTimeMillisSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys);

            JsonUtil.AssertValue(jToken, "logicalType", "time-millis");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = Parse(type, namedTypes);
            return new TimeMillisSchema(underlyingType);
        }

        private static Schema ParseTimeMicrosSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys);

            JsonUtil.AssertValue(jToken, "logicalType", "time-micros");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = Parse(type, namedTypes);
            return new TimeMicrosSchema(underlyingType);
        }

        private static Schema ParseTimeNanosSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys);

            JsonUtil.AssertValue(jToken, "logicalType", "time-nanos");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = Parse(type, namedTypes);
            return new TimeNanosSchema(underlyingType);
        }

        private static Schema ParseTimestampMillisSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys);

            JsonUtil.AssertValue(jToken, "logicalType", "timestamp-millis");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = Parse(type, namedTypes);
            return new TimestampMillisSchema(underlyingType);
        }

        private static Schema ParseTimestampMicrosSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys);

            JsonUtil.AssertValue(jToken, "logicalType", "timestamp-micros");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = Parse(type, namedTypes);
            return new TimestampMicrosSchema(underlyingType);
        }

        private static Schema ParseTimestampNanosSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys);

            JsonUtil.AssertValue(jToken, "logicalType", "timestamp-nanos");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = Parse(type, namedTypes);
            return new TimestampNanosSchema(underlyingType);
        }

        private static Schema ParseDurationSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys);

            JsonUtil.AssertValue(jToken, "logicalType", "duration");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = Parse(type, namedTypes);
            return new DurationSchema(underlyingType);
        }

        private static Schema ParseEnumType(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "type", "name", "symbols" };
            var optionalKeys = new HashSet<string>() { "namespace", "aliases", "doc" };
            JsonUtil.AssertKeys(jToken, keys, optionalKeys);

            JsonUtil.AssertValue(jToken, "type", "enum");
            var name = JsonUtil.GetValue<string>(jToken, "name");
            var symbols = JsonUtil.GetValue<JArray>(jToken, "symbols");

            var enumSchema = new EnumSchema(name, null, symbols.Values<string>());
            if (JsonUtil.TryGetValue<string>(jToken, "namespace", out var ns))
                enumSchema.Namespace = ns;

            if (JsonUtil.TryGetValue<JArray>(jToken, "aliases", out var aliases))
                enumSchema.Aliases = aliases.Values<string>().ToArray();

            if (JsonUtil.TryGetValue<string>(jToken, "doc", out var doc))
                enumSchema.Doc = doc;

            namedTypes.Add(enumSchema.FullName, enumSchema);

            return enumSchema;
        }

        private static Schema ParseRecordSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "type", "name", "fields" };
            var optionalKeys = new HashSet<string>() { "namespace", "aliases", "doc" };
            JsonUtil.AssertKeys(jToken, keys, optionalKeys);

            JsonUtil.AssertValues(jToken, "type", "record", "error");
            var type = JsonUtil.GetValue<string>(jToken, "type");
            var name = JsonUtil.GetValue<string>(jToken, "name");
            var fields = JsonUtil.GetValue<JArray>(jToken, "fields");

            RecordSchema recordSchema;
            if (type.ToString() == "error")
                recordSchema = new ErrorSchema(name);
            else
                recordSchema = new RecordSchema(name);

            if (JsonUtil.TryGetValue<string>(jToken, "namespace", out var ns))
                recordSchema.Namespace = ns;

            if (JsonUtil.TryGetValue<JArray>(jToken, "aliases", out var aliases))
                recordSchema.Aliases = aliases.Values<string>().ToArray();

            if (JsonUtil.TryGetValue<string>(jToken, "doc", out var doc))
                recordSchema.Doc = doc;

            namedTypes.Add(recordSchema.FullName, recordSchema);

            foreach (var field in ParseRecordFieldSchema(fields, namedTypes))
                recordSchema.Add(field);

            return recordSchema;
        }

        private static IEnumerable<RecordSchema.Field> ParseRecordFieldSchema(JArray jArray, IDictionary<string, NamedSchema> namedTypes)
        {
            var keys = new HashSet<string>() { "type", "name" };
            var optionalKeys = new HashSet<string>() { "aliases", "doc", "default", "order" };
            foreach (var jToken in jArray)
            {
                JsonUtil.AssertKeys(jToken, keys, optionalKeys);

                var name = JsonUtil.GetValue<string>(jToken, "name");
                var type = JsonUtil.GetValue<JToken>(jToken, "type");

                var fieldType = Parse(type, namedTypes);
                var recordFieldSchema = new RecordSchema.Field(name, fieldType);

                if (JsonUtil.TryGetValue<JArray>(jToken, "aliases", out var aliases))
                    recordFieldSchema.Aliases = aliases.Values<string>().ToArray();

                if (JsonUtil.TryGetValue<string>(jToken, "doc", out var doc))
                    recordFieldSchema.Doc = doc;

                if (JsonUtil.TryGetValue<JToken>(jToken, "default", out var def))
                    recordFieldSchema.Default = def;

                if (JsonUtil.TryGetValue<string>(jToken, "order", out var order))
                    recordFieldSchema.Order = order;

                yield return recordFieldSchema;
            }
        }

        private static Schema ParsePrimitiveSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes)
        {
            var type = string.Empty;

            if (jToken.Type == JTokenType.String)
                type = jToken.ToString();

            switch (type)
            {
                case "null":
                    return new NullSchema();
                case "boolean":
                    return new BooleanSchema();
                case "int":
                    return new IntSchema();
                case "long":
                    return new LongSchema();
                case "float":
                    return new FloatSchema();
                case "double":
                    return new DoubleSchema();
                case "bytes":
                    return new BytesSchema();
                case "string":
                    return new StringSchema();
                default:
                    if (namedTypes.TryGetValue(type, out var schema))
                        return schema;
                    else
                        throw new SchemaParseException($"Unknown or unsupported type or reference: {jToken.ToString()}");
            }
        }

        private static string JSonEncodeString(string jString)
        {
            var trimmed = jString?.Trim() ?? string.Empty;
            if (trimmed.StartsWith('[') || trimmed.StartsWith('{') || trimmed.StartsWith('"'))
                return trimmed;
            return $"\"{trimmed}\"";
        }
    }
}
