using System;

namespace Avro
{
    public class AvroException : Exception
    {
        public AvroException()
            : base() { }
        public AvroException(string message)
            : base(message) { }
        public AvroException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}
