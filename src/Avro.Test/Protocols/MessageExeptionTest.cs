using Avro;
using Avro.Protocol;
using Avro.Protocol.Schema;
using Avro.Schema;
using NUnit.Framework;

namespace Avro.Test.Protocols
{
    [TestFixture]
    public class MessageExeptionTest
    {
        [TestCase]
        public void AddParameter()
        {
            var message = new MessageSchema("A");
            var parameter = new ParameterSchema("X", new RecordSchema("Y"));

            Assert.DoesNotThrow(
                () => message.AddParameter(parameter)
            );

            Assert.Throws(
                typeof(AvroException),
                () => message.AddParameter(parameter)
            );
        }

        [TestCase]
        public void AddError()
        {
            var message = new MessageSchema("A");
            var error = new ErrorSchema("X");

            Assert.DoesNotThrow(
                () => message.AddError(error)
            );

            Assert.Throws(
                typeof(AvroException),
                () => message.AddError(error)
            );
        }
    }
}
