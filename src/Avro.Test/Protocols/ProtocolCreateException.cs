using Avro;
using Avro.Protocol;
using Avro.Protocol.Schema;
using Avro.Schema;
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
            var message = new MessageSchema("M");

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
            var message = new MessageSchema("M");

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
