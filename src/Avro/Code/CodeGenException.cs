using System;

namespace Avro.Code
{
    public class CodeGenException : AvroException
    {
        public CodeGenException()
            : base() { }

        public CodeGenException(string message)
            : base(message) { }

        public CodeGenException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}
