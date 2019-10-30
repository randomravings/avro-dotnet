using System;

namespace Avro
{
    public class AvroParseException : AvroException
    {
        public AvroParseException()
            : base() { }

        public AvroParseException(string message)
            : base(message) { }

        public AvroParseException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}
