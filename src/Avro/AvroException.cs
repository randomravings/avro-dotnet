using System;

namespace Avro
{
    public class AvroException : Exception
    {
        public AvroException(string message)
            : base(message) { }
    }
}
