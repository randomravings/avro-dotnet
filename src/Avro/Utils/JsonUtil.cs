using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Utils
{
    internal static class JsonUtil
    {
        internal static void AssertKeys(JToken jToken, ISet<string> keys, ISet<string> optionalKeys = null)
        {
            if (jToken == null || jToken.Type != JTokenType.Object)
                throw new ArgumentNullException("jToken", "jToken cannot be null and must be a JSON Object.");

            if (keys == null || keys.Count < 1)
                throw new ArgumentNullException("keys", "keys cannot be null and must contain one or more items.");

            var tokenKeys = (jToken as JObject).Properties().Select(r => r.Name);

            var missingKeys = keys.Except(tokenKeys);
            if (missingKeys.Count() > 0)
                throw new ArgumentNullException($"Missing keys [{string.Join(",", missingKeys)}]");

            var allKeys = keys;
            if (optionalKeys != null)
                allKeys = keys.Union(optionalKeys).ToHashSet();

            var unexpectedKeys = tokenKeys.Except(allKeys);
            if (unexpectedKeys.Count() > 0)
                throw new ArgumentException($"Unexpected keys [{string.Join(",", unexpectedKeys)}]");
        }

        internal static void AssertValue<T>(JToken jToken, string key, T expectedVaue)
        {
            var value = GetToken(jToken, key, true, JTokenType.String).ToString();
            if (!Equals(value, expectedVaue))
                throw new ArgumentNullException($"Expected value for '{key}': '{expectedVaue}' - was '{value}'.");
        }

        internal static void AssertValues<T>(JToken jToken, string key, params T[] expectedVaues)
        {
            var value = GetToken(jToken, key, true, JTokenType.String).ToString();
            foreach (var expectedVaue in expectedVaues)
                if (Equals(value, expectedVaue))
                    return;
            throw new ArgumentNullException($"Expected value for '{key}': '{string.Join(",", expectedVaues)}' - was '{value}'.");
        }

        internal static T GetValue<T>(JToken jToken, string key)
        {
            try
            {
                return GetToken(jToken, key, true).Value<T>();
            }
            catch (InvalidCastException ex)
            {
                throw new ArgumentException($"Unexpected JSON token type for '{key}', expected: '{nameof(T)}'", ex);
            }
        }

        internal static bool TryGetValue<T>(JToken jToken, string key, out T value)
        {
            value = default;
            var temp = GetToken(jToken, key, false);
            if (temp == null)
                return false;
            value = temp.Value<T>();
            return true;
        }

        internal static IDictionary<string, JToken> GetKeyValues(JToken jToken)
        {
            if (jToken == null || jToken.Type != JTokenType.Object)
                throw new ArgumentNullException("jToken", "jToken cannot be null and must be a JSON Object.");
            return (jToken as JObject).ToObject<IDictionary<string, JToken>>(); 
        }

        private static JToken GetToken(JToken jToken, string key, bool required, params JTokenType[] expectedTypes)
        {
            if (jToken == null)
                throw new ArgumentNullException("jToken", "jToken cannot be null.");

            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException("key", "key cannot be null.");

            var value = jToken[key];

            if (value == null)
                if (required)
                    throw new ArgumentNullException($"Key not found: '{key}'");
                else
                    return null;

            if (expectedTypes != null && expectedTypes.Length > 0)
                if (!expectedTypes.Contains(value.Type))
                    throw new ArgumentException($"Value type for '{key}' is not in [{string.Join(",", expectedTypes)}]");

            return value;
        }
    }
}
