using Avro;
using Avro.Protocols;
using Avro.Schemas;
using NUnit.Framework;

namespace Avro.Test.Protocols
{
    [TestFixture]
    public class ProtocolCreateException
    {
        [TestCase]
        public void DuplicateType()
        {
            var protocol = new AvroProtocol();
            var record = new RecordSchema("Name");

            Assert.DoesNotThrow(
                () => protocol.AddType(record)
            );

            Assert.Throws(
                typeof(AvroException),
                () => protocol.AddType(record)
            );
        }


        [TestCase]
        public void MissingMessageType()
        {
            var protocol = new AvroProtocol();
            var record = new RecordSchema("Name");
            var message = new Message("M");

            message.AddParameter(new ParameterSchema("par", record.FullName));

            Assert.Throws(
                typeof(AvroException),
                () => protocol.AddMessage(message)
            );
        }

        [TestCase]
        public void DuplicateMessage()
        {
            var protocol = new AvroProtocol();
            var record = new RecordSchema("Name");
            var message = new Message("M");

            protocol.AddType(record);
            message.AddParameter(new ParameterSchema("par", record.FullName));

            Assert.DoesNotThrow(
                () => protocol.AddMessage(message)
            );

            Assert.Throws(
                typeof(AvroException),
                () => protocol.AddMessage(message)
            );
        }
    }
}
