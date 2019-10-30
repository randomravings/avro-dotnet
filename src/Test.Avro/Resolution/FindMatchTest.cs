using Avro.Resolution;
using Avro.Schema;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;

namespace Test.Avro.Resolution
{
    [TestFixture]
    public class FindMatchTest
    {
        public void TestUnion()
        {
            var union = new UnionSchema(new StringSchema(), new IntSchema());
            var (y, x) = SchemaResolver.FindMatch(new BytesSchema(), union);

            Assert.AreEqual(1, y);
            Assert.AreEqual(new StringSchema(), x);
        }
    }
}
