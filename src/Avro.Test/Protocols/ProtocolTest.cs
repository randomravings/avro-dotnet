using Avro;
using NUnit.Framework;

namespace Avro.Test.Protocols
{
    [TestFixture]
    public class ProtocolTest
    {
        [TestCase]
        public void DocTest()
        {
            var protocol = new AvroProtocol()
            {
                Doc = "Test Doc"
            };
            Assert.AreEqual("Test Doc", protocol.Doc);
        }

        [TestCase]
        public void EqualTest()
        {
            var protocol01 = new AvroProtocol("SomeProtocol", "Some.Namespace");
            var protocol02 = new AvroProtocol("Some.Namespace.SomeProtocol");

            Assert.AreEqual(protocol01, protocol02);
        }
    }
}
