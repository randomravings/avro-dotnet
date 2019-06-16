using System;

namespace Avro.Protocols
{
    public class ProtocolParseException : AvroException
    {
        public ProtocolParseException(string message)
            : base(message) { }
        public ProtocolParseException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}
