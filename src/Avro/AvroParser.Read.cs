using Avro.Protocol;
using Avro.Protocol.Schema;
using Avro.Schema;
using Avro.Utils;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Linq;

namespace Avro
{
    public static partial class AvroParser
    {
        private static AvroSchema ParseSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace) =>
            jToken.Type switch
            {
                JTokenType.String => ParseJsonString(jToken.Value<string>(), namedTypes, enclosingNamespace),
                JTokenType.Array => ParseJsonArray(jToken.Value<JArray>(), namedTypes, enclosingNamespace),
                JTokenType.Object => ParseJsonObject(jToken.Value<JObject>(), namedTypes, enclosingNamespace),
                _ => throw new AvroParseException($"Unexpected Json token: '{jToken.Type}'"),
            };

        private static AvroSchema ParseJsonArray(JArray jArray, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace) =>
            new UnionSchema(jArray.Select(r => ParseSchema(r, namedTypes, enclosingNamespace)));

        private static AvroSchema ParseJsonObject(JObject jObject, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace) =>
            (jObject["logicalType"], jObject["type"]) switch
            {
                (JToken l, _) when l.Type == JTokenType.String => ParseLogicalSchema(jObject, namedTypes, enclosingNamespace),
                (_, JToken t) when t.Type == JTokenType.Array => ParseJsonArray(t.Value<JArray>(), namedTypes, enclosingNamespace),
                (_, JToken t) when t.Type == JTokenType.String => t.Value<string>() switch
                {
                    "array" => ParseArraySchema(jObject, namedTypes, enclosingNamespace),
                    "map" => ParseMapSchema(jObject, namedTypes, enclosingNamespace),
                    "fixed" => ParseFixedSchema(jObject, namedTypes, enclosingNamespace),
                    "enum" => ParseEnumType(jObject, namedTypes, enclosingNamespace),
                    "record" => ParseRecordSchema(jObject, namedTypes, enclosingNamespace),
                    "error" => ParseRecordSchema(jObject, namedTypes, enclosingNamespace),
                    _ => ParseJsonString(t.Value<string>(), namedTypes, enclosingNamespace)
                },
                _ => throw new AvroParseException("Object does not contain a type or logicalType key")
            };

        private static AvroSchema ParseLogicalSchema(JObject jObject, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace) =>
            (jObject["logicalType"].Value<string>()) switch
            {
                "decimal" => ParseDecimalSchema(jObject, namedTypes, enclosingNamespace),
                "time-millis" => ParseTimeMillisSchema(jObject, namedTypes, enclosingNamespace),
                "time-micros" => ParseTimeMicrosSchema(jObject, namedTypes, enclosingNamespace),
                "time-nanos" => ParseTimeNanosSchema(jObject, namedTypes, enclosingNamespace),
                "timestamp-millis" => ParseTimestampMillisSchema(jObject, namedTypes, enclosingNamespace),
                "timestamp-micros" => ParseTimestampMicrosSchema(jObject, namedTypes, enclosingNamespace),
                "timestamp-nanos" => ParseTimestampNanosSchema(jObject, namedTypes, enclosingNamespace),
                "duration" => ParseDurationSchema(jObject, namedTypes, enclosingNamespace),
                "uuid" => ParseUuidSchema(jObject, namedTypes, enclosingNamespace),
                _ => ParseSchema(jObject["type"], namedTypes, enclosingNamespace),
            };

        private static AvroSchema ParseArraySchema(JObject jObject, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace) =>
            jObject["items"] switch
            {
                JToken t => new ArraySchema(ParseSchema(t, namedTypes, enclosingNamespace)),
                _ => throw new AvroParseException("Missing 'items' from array definition")
            };

        private static AvroSchema ParseMapSchema(JObject jObject, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace) =>
            jObject["values"] switch
            {
                JToken t => new MapSchema(ParseSchema(t, namedTypes, enclosingNamespace)),
                _ => throw new AvroParseException("Missing 'values' from map definition")
            };

        private static AvroSchema ParseFixedSchema(JObject jObject, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "type", "size", "name" };
            var optionalKeys = new HashSet<string>() { "namespace" };
            JsonUtil.AssertKeys(jObject, keys, optionalKeys, out var tags);

            JsonUtil.AssertValue(jObject, "type", "fixed");
            var size = JsonUtil.GetValue<int>(jObject, "size");
            var name = JsonUtil.GetValue<string>(jObject, "name");
            var fixedSchema = new FixedSchema(name, string.Empty, size);
            if (fixedSchema.Namespace == string.Empty && JsonUtil.TryGetValue<string>(jObject, "namespace", out var ns))
                fixedSchema.Namespace = ns;
            if (string.IsNullOrEmpty(fixedSchema.Namespace))
                fixedSchema.Namespace = enclosingNamespace.Peek();
            fixedSchema.AddTags(tags);
            namedTypes.Add(fixedSchema.FullName, fixedSchema);
            return fixedSchema;
        }

        private static AvroSchema ParseDecimalSchema(JObject jObject, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "logicalType", "type", "precision", "scale" };
            JsonUtil.AssertKeys(jObject, keys, new HashSet<string>(), out var tags);

            JsonUtil.AssertValue(jObject, "logicalType", "decimal");
            var type = JsonUtil.GetValue<JToken>(jObject, "type");
            var precision = JsonUtil.GetValue<int>(jObject, "precision");
            var scale = JsonUtil.GetValue<int>(jObject, "scale");
            var underlyingType = ParseSchema(type, namedTypes, enclosingNamespace);
            var decimalSchema = new DecimalSchema(underlyingType, precision, scale);
            decimalSchema.AddTags(tags);
            return decimalSchema;
        }

        private static AvroSchema ParseUuidSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys, new HashSet<string>(), out var tags);

            JsonUtil.AssertValue(jToken, "logicalType", "uuid");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = ParseSchema(type, namedTypes, enclosingNamespace);
            var uuidSchema = new UuidSchema(underlyingType);
            uuidSchema.AddTags(tags);
            return uuidSchema;
        }

        private static AvroSchema ParseTimeMillisSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys, new HashSet<string>(), out var tags);

            JsonUtil.AssertValue(jToken, "logicalType", "time-millis");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = ParseSchema(type, namedTypes, enclosingNamespace);
            var timeMillisSchema = new TimeMillisSchema(underlyingType);
            timeMillisSchema.AddTags(tags);
            return timeMillisSchema;
        }

        private static AvroSchema ParseTimeMicrosSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys, new HashSet<string>(), out var tags);

            JsonUtil.AssertValue(jToken, "logicalType", "time-micros");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = ParseSchema(type, namedTypes, enclosingNamespace);
            var timeMicrosSchema = new TimeMicrosSchema(underlyingType);
            timeMicrosSchema.AddTags(tags);
            return timeMicrosSchema;
        }

        private static AvroSchema ParseTimeNanosSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys, new HashSet<string>(), out var tags);

            JsonUtil.AssertValue(jToken, "logicalType", "time-nanos");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = ParseSchema(type, namedTypes, enclosingNamespace);
            var timeNanosSchema = new TimeNanosSchema(underlyingType);
            timeNanosSchema.AddTags(tags);
            return timeNanosSchema;
        }

        private static AvroSchema ParseTimestampMillisSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys, new HashSet<string>(), out var tags);

            JsonUtil.AssertValue(jToken, "logicalType", "timestamp-millis");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = ParseSchema(type, namedTypes, enclosingNamespace);
            var timestampMillisSchema = new TimestampMillisSchema(underlyingType);
            timestampMillisSchema.AddTags(tags);
            return timestampMillisSchema;
        }

        private static AvroSchema ParseTimestampMicrosSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys, new HashSet<string>(), out var tags);

            JsonUtil.AssertValue(jToken, "logicalType", "timestamp-micros");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = ParseSchema(type, namedTypes, enclosingNamespace);
            var timestampMicrosSchema = new TimestampMicrosSchema(underlyingType);
            timestampMicrosSchema.AddTags(tags);
            return timestampMicrosSchema;
        }

        private static AvroSchema ParseTimestampNanosSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys, new HashSet<string>(), out var tags);

            JsonUtil.AssertValue(jToken, "logicalType", "timestamp-nanos");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = ParseSchema(type, namedTypes, enclosingNamespace);
            var timestampNanosSchema = new TimestampNanosSchema(underlyingType);
            timestampNanosSchema.AddTags(tags);
            return timestampNanosSchema;
        }

        private static AvroSchema ParseDurationSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys, new HashSet<string>(), out var tags);

            JsonUtil.AssertValue(jToken, "logicalType", "duration");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var underlyingType = ParseSchema(type, namedTypes, enclosingNamespace);
            var durationSchema = new DurationSchema(underlyingType);
            durationSchema.AddTags(tags);
            return durationSchema;
        }

        private static AvroSchema ParseEnumType(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "type", "name", "symbols" };
            var optionalKeys = new HashSet<string>() { "namespace", "aliases", "doc" };
            JsonUtil.AssertKeys(jToken, keys, optionalKeys, out var tags);

            JsonUtil.AssertValue(jToken, "type", "enum");
            var name = JsonUtil.GetValue<string>(jToken, "name");
            var symbols = JsonUtil.GetValue<JArray>(jToken, "symbols");

            var enumSchema = new EnumSchema(name, string.Empty, symbols.Values<string>());
            enumSchema.AddTags(tags);
            if (enumSchema.Namespace == string.Empty && JsonUtil.TryGetValue<string>(jToken, "namespace", out var ns))
                enumSchema.Namespace = ns;

            if (string.IsNullOrEmpty(enumSchema.Namespace))
                enumSchema.Namespace = enclosingNamespace.Peek();

            if (JsonUtil.TryGetValue<JArray>(jToken, "aliases", out var aliases))
                enumSchema.Aliases = aliases.Values<string>().ToArray();

            if (JsonUtil.TryGetValue<string>(jToken, "doc", out var doc))
                enumSchema.Doc = doc;

            namedTypes.Add(enumSchema.FullName, enumSchema);

            return enumSchema;
        }

        private static AvroSchema ParseRecordSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "type", "name", "fields" };
            var optionalKeys = new HashSet<string>() { "namespace", "aliases", "doc" };
            JsonUtil.AssertKeys(jToken, keys, optionalKeys, out var tags);

            JsonUtil.AssertValues(jToken, "type", "record", "error");
            var type = JsonUtil.GetValue<string>(jToken, "type");
            var name = JsonUtil.GetValue<string>(jToken, "name");
            var fields = JsonUtil.GetValue<JArray>(jToken, "fields");

            var recordSchema = new RecordSchema(name);
            if (type.ToString() == "error")
                recordSchema = new ErrorSchema(name);
            recordSchema.AddTags(tags);

            if (recordSchema.Namespace == string.Empty && JsonUtil.TryGetValue<string>(jToken, "namespace", out var ns))
                recordSchema.Namespace = ns;

            if (string.IsNullOrEmpty(recordSchema.Namespace))
                recordSchema.Namespace = enclosingNamespace.Peek();

            enclosingNamespace.Push(recordSchema.Namespace);

            if (JsonUtil.TryGetValue<JArray>(jToken, "aliases", out var aliases))
                recordSchema.Aliases = aliases.Values<string>().ToArray();

            if (JsonUtil.TryGetValue<string>(jToken, "doc", out var doc))
                recordSchema.Doc = doc;

            namedTypes.Add(recordSchema.FullName, recordSchema);

            foreach (var field in ParseRecordFieldSchema(fields, namedTypes, enclosingNamespace))
                recordSchema.Add(field);

            enclosingNamespace.Pop();

            return recordSchema;
        }

        private static IList<RecordFieldSchema> ParseRecordFieldSchema(JArray jArray, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "type", "name" };
            var optionalKeys = new HashSet<string>() { "aliases", "doc", "default", "order" };
            var fields = new List<RecordFieldSchema>();
            foreach (var jToken in jArray)
            {
                JsonUtil.AssertKeys(jToken, keys, optionalKeys, out var tags);

                var name = JsonUtil.GetValue<string>(jToken, "name");
                var type = JsonUtil.GetValue<JToken>(jToken, "type");

                var fieldType = ParseSchema(type, namedTypes, enclosingNamespace);
                var recordFieldSchema = new RecordFieldSchema(name, fieldType);
                recordFieldSchema.AddTags(tags);

                if (JsonUtil.TryGetValue<JArray>(jToken, "aliases", out var aliases))
                    recordFieldSchema.Aliases = aliases.Values<string>().ToArray();

                if (JsonUtil.TryGetValue<string>(jToken, "doc", out var doc))
                    recordFieldSchema.Doc = doc;

                if (JsonUtil.TryGetValue<JToken>(jToken, "default", out var def))
                    recordFieldSchema.Default = def;

                if (JsonUtil.TryGetValue<string>(jToken, "order", out var order))
                    recordFieldSchema.Order = order;

                fields.Add(recordFieldSchema);
            }
            return fields;
        }

        private static AvroSchema ParseJsonString(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
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
                    type = QualifyName(type, enclosingNamespace);
                    if (namedTypes.TryGetValue(type, out var schema))
                        return schema;
                    else
                        throw new AvroParseException($"Unknown or unsupported type or reference: {jToken.ToString()}");
            }
        }

        private static AvroProtocol ParseProtocol(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            switch (jToken.Type)
            {
                case JTokenType.Object:
                    var keys = new HashSet<string>() { "protocol" };
                    var optionalKeys = new HashSet<string>() { "namespace", "doc", "types", "messages" };
                    JsonUtil.AssertKeys(jToken, keys, optionalKeys, out _);

                    var name = JsonUtil.GetValue<string>(jToken, "protocol");
                    var protocol = new AvroProtocol(name);

                    if (JsonUtil.TryGetValue<string>(jToken, "namespace", out var ns))
                        protocol.Namespace = ns;

                    if (string.IsNullOrEmpty(protocol.Namespace))
                        protocol.Namespace = enclosingNamespace.Peek();

                    enclosingNamespace.Push(protocol.Namespace);

                    if (JsonUtil.TryGetValue<string>(jToken, "doc", out var doc))
                        protocol.Doc = doc;
                    if (JsonUtil.TryGetValue<JArray>(jToken, "types", out var types))
                        foreach (var type in ParseProtocolTypes(types, namedTypes, enclosingNamespace))
                            protocol.AddType(type);
                    if (JsonUtil.TryGetValue<JObject>(jToken, "messages", out var messages))
                        foreach (var message in ParseMessages(messages, protocol.Types.ToDictionary(r => r.FullName), enclosingNamespace))
                            protocol.AddMessage(message);

                    enclosingNamespace.Pop();

                    return protocol;
                default:
                    throw new AvroParseException($"Unexpected Json token: '{jToken.Type}'");
            }
        }

        private static IList<NamedSchema> ParseProtocolTypes(JArray jArray, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var namedSchemas = new List<NamedSchema>();
            foreach (var item in jArray)
            {
                var type = ParseSchema(item, namedTypes, enclosingNamespace);
                if (!(type is NamedSchema))
                    throw new AvroParseException($"Unexpected type for protocol. Expected [record, error, enum or fixed], was: '{type.ToString()}'.");
                namedSchemas.Add((NamedSchema)type);
            }
            return namedSchemas;
        }

        private static IList<MessageSchema> ParseMessages(JObject jObject, IDictionary<string, NamedSchema> types, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "request", "response" };
            var optionalKeys = new HashSet<string>() { "doc", "errors", "one-way" };
            var messages = new List<MessageSchema>();
            foreach (var item in JsonUtil.GetKeyValues(jObject))
            {
                var name = item.Key;
                var jToken = item.Value;

                JsonUtil.AssertKeys(item.Value, keys, optionalKeys, out _);

                var message = new MessageSchema(name);
                var request = JsonUtil.GetValue<JArray>(jToken, "request");
                var response = JsonUtil.GetValue<JToken>(jToken, "response");

                foreach (var requestParameter in ParseRequests(request, types, enclosingNamespace))
                    message.AddParameter(requestParameter);
                message.Response = ParseSchema(response, types, enclosingNamespace);

                if (JsonUtil.TryGetValue<string>(jToken, "doc", out var doc))
                    message.Doc = doc;
                if (JsonUtil.TryGetValue<bool>(jToken, "one-way", out var oneway))
                    if (oneway && !(message.Response is NullSchema))
                        throw new AvroParseException("One way messages must have a 'null' response");
                    else
                        message.Oneway = oneway;

                if (JsonUtil.TryGetValue<JArray>(jToken, "errors", out var errors))
                {
                    foreach (var error in errors)
                    {
                        if (error.Type != JTokenType.String)
                            throw new AvroParseException($"Declared type must be a string.");

                        var declaredType = QualifyName(error.ToString(), enclosingNamespace);
                        if (!types.TryGetValue(declaredType, out var errorType))
                            throw new AvroParseException($"'{declaredType}' is not a declared type.");
                        if (!(errorType is ErrorSchema))
                            throw new AvroParseException($"'{declaredType}' is not an error type.");
                        message.AddError((ErrorSchema)errorType);
                    }
                }

                messages.Add(message);
            }
            return messages;
        }

        private static IList<ParameterSchema> ParseRequests(JArray jArray, IDictionary<string, NamedSchema> types, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "name", "type" };
            var requests = new List<ParameterSchema>();
            foreach (var item in jArray)
            {
                JsonUtil.AssertKeys(item, keys, new HashSet<string>(), out _);
                var name = JsonUtil.GetValue<string>(item, "name");
                var type = JsonUtil.GetValue<string>(item, "type");

                type = QualifyName(type, enclosingNamespace);
                if (!types.TryGetValue(type, out var request))
                    throw new AvroParseException($"Unknown request parameter type: '{type}'.");
                requests.Add(new ParameterSchema(name, request));
            }
            return requests;
        }
    }
}
