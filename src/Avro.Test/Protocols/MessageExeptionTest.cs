using Avro;
using Avro.Protocols;
using Avro.Schemas;
using NUnit.Framework;

namespace Avro.Test.Protocols
{
    [TestFixture]
    public class MessageExeptionTest
    {
        [TestCase]
        public void AddParameter()
        {
            var message = new Message("A");
            var parameter = new RequestParameter("X", new RecordSchema("Y"));

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
            var message = new Message("A");
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
