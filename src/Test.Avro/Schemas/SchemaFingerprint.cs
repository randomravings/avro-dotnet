using Avro;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Test.Avro.Schema
{
    [TestFixture]
    public class SchemaFingerprint
    {
        [Test, TestCaseSource(typeof(SchemaFingerprints))]
        public void TestFingerprint(string avro, long expectedFp)
        {
            var s = AvroParser.ReadSchema(avro);
            var fp = AvroFingerprint.CRC64Value(s);
            Assert.AreEqual(expectedFp, fp);
        }
    }

    class SchemaFingerprints : IEnumerable
    {
        public IEnumerator GetEnumerator()
        {
            yield return new object[] { @"null", 7195948357588979594 };
            yield return new object[] { @"boolean", -6970731678124411036 };
            yield return new object[] { @"int", 8247732601305521295 };
            yield return new object[] { @"long", -3434872931120570953 };
            yield return new object[] { @"float", 5583340709985441680 };
            yield return new object[] { @"double", -8181574048448539266 };
            yield return new object[] { @"bytes", 5746618253357095269 };
            yield return new object[] { @"string", -8142146995180207161 };
            yield return new object[] { @"[]", -1241056759729112623 };
            yield return new object[] { @"[""int""]", -5232228896498058493 };
            yield return new object[] { @"[""int"",""boolean""]", 5392556393470105090 };
            yield return new object[] { @"{""name"":""x.y.foo"",""type"":""record"",""fields"":[]}", 5916914534497305771 };
            yield return new object[] { @"{""name"":""a.b.foo"",""type"":""record"",""fields"":[]}", -4616218487480524110 };
            yield return new object[] { @"{""name"":""foo"",""type"":""record"",""fields"":[]}", -4824392279771201922 };
            yield return new object[] { @"{""name"":""foo"",""type"":""record"",""fields"":[{""name"":""f1"",""type"":""boolean""}]}", 7843277075252814651 };
            yield return new object[] { @"{""name"":""foo"",""type"":""record"",""fields"":[{""name"":""f1"",""type"":""boolean""},{""name"":""f2"",""type"":""int""}]}", -4860222112080293046 };
            yield return new object[] { @"{""name"":""foo"",""type"":""enum"",""symbols"":[""A1""]}", -6342190197741309591 };
            yield return new object[] { @"{""name"":""x.y.z.foo"",""type"":""enum"",""symbols"":[""A1"",""A2""]}", -4448647247586288245 };
            yield return new object[] { @"{""name"":""foo"",""type"":""fixed"",""size"":15}", 1756455273707447556 };
            yield return new object[] { @"{""name"":""x.y.z.foo"",""type"":""fixed"",""size"":32}", -3064184465700546786 };
            yield return new object[] { @"{""type"":""array"",""items"":""null""}", -589620603366471059 };
            yield return new object[] { @"{""type"":""map"",""values"":""string""}", -8732877298790414990 };
            yield return new object[] { @"{""name"":""PigValue"",""type"":""record"",""fields"":[{""name"":""value"",""type"":[""null"",""int"",""long"",""PigValue""]}]}", -1759257747318642341 };
        }
    }
}
