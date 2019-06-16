using Avro.Schemas;
using Avro.Utils;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Protocols
{
    internal static class ProtocolParser
    {
        internal static Protocol Parse(string text)
        {
            var json = JToken.Parse(text);
            return Parse(json);
        }

        private static Protocol Parse(JToken jToken)
        {
            switch (jToken.Type)
            {
                case JTokenType.Object:
                    return ParseProtocol(jToken);
                default:
                    throw new ProtocolParseException($"Unexpected Json token: '{jToken.Type}'");
            }
        }

        private static Protocol ParseProtocol(JToken jToken)
        {
            var keys = new HashSet<string>() { "protocol" };
            var optionalKeys = new HashSet<string>() { "namespace", "doc", "types", "messages" };
            JsonUtil.AssertKeys(jToken, keys, optionalKeys);

            var name = JsonUtil.GetValue<string>(jToken, "protocol");
            var protocol = new Protocol(name);

            if (JsonUtil.TryGetValue<string>(jToken, "namespace", out var ns))
                protocol.Namespace = ns;
            if (JsonUtil.TryGetValue<string>(jToken, "doc", out var doc))
                protocol.Doc = doc;
            if (JsonUtil.TryGetValue<JArray>(jToken, "types", out var types))
                protocol.Types = ParseSchemas(types).ToList();
            if (JsonUtil.TryGetValue<JObject>(jToken, "messages", out var messages))
                protocol.Messages = ParseMessages(messages, protocol.Types.ToDictionary(r => r.FullName)).ToList();


            return protocol;
        }

        private static IEnumerable<NamedSchema> ParseSchemas(JArray jArray)
        {
            var namedTypes = new Dictionary<string, NamedSchema>();
            foreach (var item in jArray)
            {
                var type = SchemaParser.Parse(item, namedTypes);
                if (!(type is NamedSchema))
                    throw new ProtocolParseException($"Unexpected type for protocol. Expected [record, error, enum or fixed], was: '{type.ToString()}'.");
                var namedSchema = type as NamedSchema;
                yield return namedSchema;
            }
        }

        private static IEnumerable<Message> ParseMessages(JObject jObject, IDictionary<string, NamedSchema> types)
        {
            var keys = new HashSet<string>() { "request", "response" };
            var optionalKeys = new HashSet<string>() { "doc", "errors", "one-way" };

            foreach (var item in JsonUtil.GetKeyValues(jObject))
            {
                var name = item.Key;
                var jToken = item.Value;

                JsonUtil.AssertKeys(item.Value, keys, optionalKeys);

                var message = new Message(name);
                var request = JsonUtil.GetValue<JArray>(jToken, "request");
                var response = JsonUtil.GetValue<JToken>(jToken, "response");
                var errorTypes = new UnionSchema(new StringSchema());

                message.Request = ParseRequests(request, types).ToList();
                message.Response = SchemaParser.Parse(response, types);

                if (JsonUtil.TryGetValue<string>(jToken, "doc", out var doc))
                    message.Doc = doc;
                if (JsonUtil.TryGetValue<bool>(jToken, "one-way", out var oneway))
                    if (oneway && !(message.Response is NullSchema))
                        throw new ProtocolParseException("One way messages must have a 'null' response");
                    else
                        message.Oneway = oneway;

                if (JsonUtil.TryGetValue<JArray>(jToken, "errors", out var errors))
                {
                    foreach (var error in errors)
                    {
                        if (error.Type != JTokenType.String)
                            throw new ProtocolParseException($"Declared type must be a string.");

                        var declaredType = error.ToString();
                        if (!types.TryGetValue(declaredType, out var errorType))
                            throw new ProtocolParseException($"'{declaredType}' is not a declared type.");
                        if (!(errorType is ErrorSchema))
                            throw new ProtocolParseException($"'{declaredType}' is not an error type.");
                        errorTypes.Add(errorType);
                    }
                }

                yield return message;
            }
        }

        private static IEnumerable<RecordSchema> ParseRequests(JArray jArray, IDictionary<string, NamedSchema> types)
        {
            var keys = new HashSet<string>() { "name", "type" };
            foreach (var item in jArray)
            {
                JsonUtil.AssertKeys(item, keys);
                var name = JsonUtil.GetValue<string>(item, "name");
                var type = JsonUtil.GetValue<string>(item, "type");

                if (!types.TryGetValue(type, out var request))
                    throw new ProtocolParseException($"Unknown request parameter type: '{type}'.");
                if (!(request is RecordSchema))
                    throw new ProtocolParseException($"Request '{type}' is not a record schema.");
                yield return request as RecordSchema;
            }
        }
    }
}

