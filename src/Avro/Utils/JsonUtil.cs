using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Utils
{
    public static class JsonUtil
    {
        public static JToken EmptyDefault { get; private set; } = JToken.Parse("{}");

        public static void AssertKeys(JToken jToken, ISet<string> keys, ISet<string> optionalKeys, out IDictionary<string, object> additionalTags)
        {
            additionalTags = new Dictionary<string, object>();
            if (jToken.Type != JTokenType.Object)
                throw new ArgumentException("jToken", "jToken must be a JSON Object.");

            if (keys.Count == 0)
                throw new ArgumentException("keys", "keys must contain one or more items.");

            var tokenKeys = ((JObject)jToken).Properties().Select(r => r.Name);

            var missingKeys = keys.Except(tokenKeys);
            if (missingKeys.Count() > 0)
                throw new KeyNotFoundException($"Missing keys [{string.Join(",", missingKeys)}]");

            var allKeys = keys;
            if (optionalKeys != null)
                allKeys = keys.Union(optionalKeys).ToHashSet();

            var additionalKeys = tokenKeys.Except(allKeys);
            foreach (var additionalKey in additionalKeys)
                additionalTags.Add(additionalKey, jToken[additionalKey]);
        }

        public static void AssertValue(JToken jToken, string key, string expectedVaue)
        {
            var value = GetValue<string>(jToken, key);
            if (!Equals(value, expectedVaue))
                throw new ArgumentException($"Expected value for '{key}': '{expectedVaue}' - was '{value}'.");
        }

        public static void AssertValues(JToken jToken, string key, params string[] expectedVaues)
        {
            var value = GetValue<string>(jToken, key);
            foreach (var expectedVaue in expectedVaues)
                if (Equals(value, expectedVaue))
                    return;
            throw new ArgumentException($"Expected value for '{key}': '{string.Join(",", expectedVaues)}' - was '{value}'.");
        }

        public static T GetValue<T>(JToken jToken, string key)
        {
            return GetToken(jToken, key, true).Value<T>();
        }

        public static bool TryGetValue<T>(JToken jToken, string key, out T value)
        {
            value = default;
            var temp = GetToken(jToken, key, false);
            if (temp == null)
                return false;
            value = temp.Value<T>();
            return true;
        }

        //public static (bool, T) TryGetValue<T>(JToken jToken, string key)
        //    => GetToken(jToken, key, false) switch
        //    {
        //        null => (false, default),
        //        var t => (true, t.Value<T>())
        //    };

        public static IDictionary<string, JToken> GetKeyValues(JToken jToken)
        {
            if (jToken.Type != JTokenType.Object)
                throw new ArgumentException("jToken", "jToken cannot be null and must be a JSON Object.");
            return ((JObject)jToken).ToObject<IDictionary<string, JToken>>();
        }

        public static JToken? GetToken(JToken jToken, string key, bool required, JTokenType? expectedType = null)
        {
            if (jToken.Type != JTokenType.Object)
                throw new ArgumentException("jToken", "jToken cannot be null and must be a JSON Object.");

            var value = jToken[key];

            if (value == null)
                if (required)
                    throw new ArgumentException($"Key not found: '{key}'");
                else
                    return null;

            if (expectedType.HasValue && expectedType.Value != value.Type)
                throw new ArgumentException($"Value type for '{key}' is not of type: {expectedType.Value}]");

            return value;
        }
    }
}
