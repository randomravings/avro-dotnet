using Avro.Utils;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;

namespace Avro.Test.Utils
{
    [TestFixture]
    public class JsonUtilTest
    {
        [TestCase]
        public void AssertKeys()
        {
            var jArray = JToken.Parse("[1, 2, 3, 4]");
            var jToken = JToken.Parse(@"{""name"":""XYZ"",""type"":""record"",""fields"":[{""name"":""A"",""type"":""int""}],""happy"":""face""}");

            Assert.Throws(
                typeof(ArgumentException),
                () => JsonUtil.AssertKeys(
                    jArray,
                    new HashSet<string>() { "name" },
                    new HashSet<string>(),
                    out _
                )
            );


            Assert.Throws(
                typeof(ArgumentException),
                () => JsonUtil.AssertKeys(
                    jToken,
                    new HashSet<string>(),
                    new HashSet<string>(),
                    out _
                )
            );

            Assert.Throws(
                typeof(KeyNotFoundException),
                () => JsonUtil.AssertKeys(
                    jToken,
                    new HashSet<string>() { "name", "outlandishkey" },
                    new HashSet<string>(),
                    out _
                )
            );

            JsonUtil.AssertKeys(
                jToken,
                new HashSet<string>() { "name", "type", "fields" },
                new HashSet<string>() { "doc", "aliases" },
                out var additionalTags
            );

            Assert.IsTrue(additionalTags.ContainsKey("happy"));
            Assert.IsInstanceOf<JValue>(additionalTags["happy"]);
            Assert.AreEqual("face", additionalTags["happy"].ToString());
        }

        [TestCase]
        public void AssertKeysGetValue()
        {
            var jToken = JToken.Parse(@"{""foo"":""bar""}");

            var value = JsonUtil.GetValue<string>(jToken, "foo");

            Assert.AreEqual("bar", value);

            Assert.Throws(
                typeof(FormatException),
                () => JsonUtil.GetValue<int>(jToken, "foo")
            );

            Assert.Throws(
                typeof(ArgumentException),
                () => JsonUtil.GetValue<int>(jToken, "bar")
            );
        }

        [TestCase]
        public void AssertValues()
        {
            var jToken = JToken.Parse(@"{""num"":""1"",""happy"":""face""}");

            Assert.DoesNotThrow(
                () => JsonUtil.AssertValue(jToken, "num", "1")
            );

            Assert.DoesNotThrow(
                () => JsonUtil.AssertValue(jToken, "happy", "face")
            );

            Assert.DoesNotThrow(
                () => JsonUtil.AssertValues(jToken, "happy", "face", "X", "Y")
            );


            Assert.Throws(
                typeof(ArgumentException),
                () => JsonUtil.AssertValue(jToken, "num", "X")
            );

            Assert.Throws(
                typeof(ArgumentException),
                () => JsonUtil.AssertValue(jToken, "q2345", "X")
            );

            Assert.Throws(
                typeof(ArgumentException),
                () => JsonUtil.AssertValues(jToken, "num", "face", "X", "Y")
            );
        }

        [TestCase]
        public void GetKeyValues()
        {
            var jArray = JToken.Parse("[1, 2, 3, 4]");
            var jToken = JToken.Parse(@"{""num"":1,""happy"":""face""}");

            Assert.Throws(
                typeof(ArgumentException),
                () => JsonUtil.GetKeyValues(jArray)
            );

            var keyValues = JsonUtil.GetKeyValues(jToken);

            Assert.AreEqual(2, keyValues.Count);

            Assert.IsTrue(keyValues.ContainsKey("num"));
            Assert.AreEqual(JTokenType.Integer, keyValues["num"].Type);
            Assert.AreEqual(1, keyValues["num"].Value<int>());

            Assert.IsTrue(keyValues.ContainsKey("happy"));
            Assert.AreEqual(JTokenType.String, keyValues["happy"].Type);
            Assert.AreEqual("face", keyValues["happy"].Value<string>());
        }

        [TestCase]
        public void GetValue()
        {
            var jToken = JToken.Parse(@"{""num"":1,""happy"":""face""}");

            Assert.Throws(
                typeof(ArgumentException),
                () => JsonUtil.GetValue<string>(jToken, "X")
            );

            Assert.Throws(
                typeof(InvalidCastException),
                () => JsonUtil.GetValue<Array>(jToken, "num")
            );

            var value = JsonUtil.GetValue<int>(jToken, "num");
            Assert.AreEqual(1, value);
        }

        [TestCase]
        public void TryGetValue()
        {
            var jToken = JToken.Parse(@"{""num"":1,""happy"":""face""}");

            Assert.Throws(
                typeof(InvalidCastException),
                () => JsonUtil.TryGetValue<Array>(jToken, "num", out _)
            );

            var falseResult = JsonUtil.TryGetValue<string>(jToken, "X", out var nullOutVariable);
            Assert.IsFalse(falseResult);
            Assert.IsNull(nullOutVariable);

            var trueResult = JsonUtil.TryGetValue<int>(jToken, "num", out var oneOutVariable);
            Assert.IsTrue(trueResult);
            Assert.AreEqual(1, oneOutVariable);
        }

        [TestCase]
        public void GetToken()
        {
            var jArray = JToken.Parse("[1, 2, 3, 4]");
            var jToken = JToken.Parse(@"{""num"":1,""happy"":""face""}");

            Assert.NotNull(JsonUtil.GetToken(jToken, "num", true));
            Assert.NotNull(JsonUtil.GetToken(jToken, "num", true, JTokenType.Integer));

            Assert.NotNull(JsonUtil.GetToken(jToken, "happy", true));
            Assert.NotNull(JsonUtil.GetToken(jToken, "happy", true, JTokenType.String));

            Assert.Null(JsonUtil.GetToken(jToken, "X", false));

            Assert.Throws(typeof(ArgumentException), () => JsonUtil.GetToken(jArray, "X", false));
            Assert.Throws(typeof(ArgumentException), () => JsonUtil.GetToken(jToken, "X", true));
            Assert.Throws(typeof(ArgumentException), () => JsonUtil.GetToken(jToken, "happy", true, JTokenType.TimeSpan));
        }
    }
}
