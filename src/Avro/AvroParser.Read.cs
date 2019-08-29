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
        private static AvroSchema ParseSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            switch (jToken.Type)
            {
                case JTokenType.Array:
                    return ParseUnionSchema(jToken, namedTypes, enclosingNamespace);
                case JTokenType.Object:
                    return ParseComplexSchema(jToken, namedTypes, enclosingNamespace);
                case JTokenType.String:
                    return ParsePrimitiveSchema(jToken.ToString(), namedTypes, enclosingNamespace);
                default:
                    throw new AvroParseException($"Unexpected Json token: '{jToken.Type}'");
            }
        }

        private static AvroSchema ParseUnionSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var jArray = jToken as JArray;
            var unionSchema = new UnionSchema();
            foreach (var item in jArray)
            {
                var schema = ParseSchema(item, namedTypes, enclosingNamespace);
                unionSchema.Add(schema);
            }
            return unionSchema;
        }

        private static AvroSchema ParseComplexSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var type = JsonUtil.GetValue<JToken>(jToken, "type");

            if (JsonUtil.TryGetValue<JToken>(jToken, "logicalType", out _))
                return ParseLogicalSchema(jToken, namedTypes, enclosingNamespace);

            if (type.Type == JTokenType.Array)
                return ParseUnionSchema(type as JArray, namedTypes, enclosingNamespace);

            switch (type.ToString())
            {
                case "array":
                    return ParseArraySchema(jToken, namedTypes, enclosingNamespace);
                case "map":
                    return ParseMapSchema(jToken, namedTypes, enclosingNamespace);
                case "fixed":
                    return ParseFixedSchema(jToken, namedTypes, enclosingNamespace);
                case "enum":
                    return ParseEnumType(jToken, namedTypes, enclosingNamespace);
                case "record":
                case "error":
                    return ParseRecordSchema(jToken, namedTypes, enclosingNamespace);
                default:
                    return ParsePrimitiveSchema(type, namedTypes, enclosingNamespace);
            }
        }

        private static AvroSchema ParseLogicalSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var logicalType = JsonUtil.GetValue<string>(jToken, "logicalType");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");

            switch (logicalType)
            {
                case "decimal":
                    return ParseDecimalSchema(jToken, namedTypes, enclosingNamespace);
                case "time-millis":
                    return ParseTimeMillisSchema(jToken, namedTypes, enclosingNamespace);
                case "time-micros":
                    return ParseTimeMicrosSchema(jToken, namedTypes, enclosingNamespace);
                case "time-nanos":
                    return ParseTimeNanosSchema(jToken, namedTypes, enclosingNamespace);
                case "timestamp-millis":
                    return ParseTimestampMillisSchema(jToken, namedTypes, enclosingNamespace);
                case "timestamp-micros":
                    return ParseTimestampMicrosSchema(jToken, namedTypes, enclosingNamespace);
                case "timestamp-nanos":
                    return ParseTimestampNanosSchema(jToken, namedTypes, enclosingNamespace);
                case "duration":
                    return ParseDurationSchema(jToken, namedTypes, enclosingNamespace);
                case "uuid":
                    return ParseUuidSchema(jToken, namedTypes, enclosingNamespace);
                default:
                    return ParseSchema(type, namedTypes, enclosingNamespace);
            }
        }

        private static AvroSchema ParseArraySchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "type", "items" };
            JsonUtil.AssertKeys(jToken, keys, null, out var tags);

            JsonUtil.AssertValue(jToken, "type", "array");
            var items = JsonUtil.GetValue<JToken>(jToken, "items");
            var itemsSchema = ParseSchema(items, namedTypes, enclosingNamespace);
            var arraySchema = new ArraySchema(itemsSchema);
            arraySchema.AddTags(tags);
            return arraySchema;
        }

        private static AvroSchema ParseMapSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "type", "values" };
            JsonUtil.AssertKeys(jToken, keys, null, out var tags);

            JsonUtil.AssertValue(jToken, "type", "map");
            var values = JsonUtil.GetValue<JToken>(jToken, "values");
            var valuesSchema = ParseSchema(values, namedTypes, enclosingNamespace);
            var mapSchema = new MapSchema(valuesSchema);
            mapSchema.AddTags(tags);
            return mapSchema;
        }

        private static AvroSchema ParseFixedSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "type", "size", "name" };
            var optionalKeys = new HashSet<string>() { "namespace" };
            JsonUtil.AssertKeys(jToken, keys, optionalKeys, out var tags);

            JsonUtil.AssertValue(jToken, "type", "fixed");
            var size = JsonUtil.GetValue<int>(jToken, "size");
            var name = JsonUtil.GetValue<string>(jToken, "name");
            JsonUtil.TryGetValue<string>(jToken, "namespace", out var ns);
            var fixedSchema = new FixedSchema(name, ns, size);
            if (string.IsNullOrEmpty(fixedSchema.Namespace))
                fixedSchema.Namespace = enclosingNamespace.Peek();
            fixedSchema.AddTags(tags);
            namedTypes.Add(fixedSchema.FullName, fixedSchema);
            return fixedSchema;
        }

        private static AvroSchema ParseDecimalSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "logicalType", "type", "precision", "scale" };
            JsonUtil.AssertKeys(jToken, keys, null, out var tags);

            JsonUtil.AssertValue(jToken, "logicalType", "decimal");
            var type = JsonUtil.GetValue<JToken>(jToken, "type");
            var precision = JsonUtil.GetValue<int>(jToken, "precision");
            var scale = JsonUtil.GetValue<int>(jToken, "scale");
            var underlyingType = ParseSchema(type, namedTypes, enclosingNamespace);
            var decimalSchema = new DecimalSchema(underlyingType, precision, scale);
            decimalSchema.AddTags(tags);
            return decimalSchema;
        }

        private static AvroSchema ParseUuidSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "logicalType", "type" };
            JsonUtil.AssertKeys(jToken, keys, null, out var tags);

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
            JsonUtil.AssertKeys(jToken, keys, null, out var tags);

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
            JsonUtil.AssertKeys(jToken, keys, null, out var tags);

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
            JsonUtil.AssertKeys(jToken, keys, null, out var tags);

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
            JsonUtil.AssertKeys(jToken, keys, null, out var tags);

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
            JsonUtil.AssertKeys(jToken, keys, null, out var tags);

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
            JsonUtil.AssertKeys(jToken, keys, null, out var tags);

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
            JsonUtil.AssertKeys(jToken, keys, null, out var tags);

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

            var enumSchema = new EnumSchema(name, null, symbols.Values<string>());
            enumSchema.AddTags(tags);
            if (JsonUtil.TryGetValue<string>(jToken, "namespace", out var ns))
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

            if (JsonUtil.TryGetValue<string>(jToken, "namespace", out var ns))
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

        private static IList<RecordSchema.Field> ParseRecordFieldSchema(JArray jArray, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
        {
            var keys = new HashSet<string>() { "type", "name" };
            var optionalKeys = new HashSet<string>() { "aliases", "doc", "default", "order" };
            var fields = new List<RecordSchema.Field>();
            foreach (var jToken in jArray)
            {
                JsonUtil.AssertKeys(jToken, keys, optionalKeys, out var tags);

                var name = JsonUtil.GetValue<string>(jToken, "name");
                var type = JsonUtil.GetValue<JToken>(jToken, "type");

                var fieldType = ParseSchema(type, namedTypes, enclosingNamespace);
                var recordFieldSchema = new RecordSchema.Field(name, fieldType);
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

        private static AvroSchema ParsePrimitiveSchema(JToken jToken, IDictionary<string, NamedSchema> namedTypes, Stack<string> enclosingNamespace)
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
                namedSchemas.Add(type as NamedSchema);
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
                        message.AddError(errorType as ErrorSchema);
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
                JsonUtil.AssertKeys(item, keys, null, out _);
                var name = JsonUtil.GetValue<string>(item, "name");
                var type = JsonUtil.GetValue<string>(item, "type");

                type = QualifyName(type, enclosingNamespace);
                if (!types.TryGetValue(type, out var request))
                    throw new AvroParseException($"Unknown request parameter type: '{type}'.");
                requests.Add(new ParameterSchema(name, request.FullName));
            }
            return requests;
        }
    }
}
