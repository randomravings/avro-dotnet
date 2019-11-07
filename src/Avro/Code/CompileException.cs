using System;

namespace Avro.Code
{
    public class CompileException : AvroException
    {
        public CompileException()
            : base() { }

        public CompileException(string message)
            : base(message) { }

        public CompileException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}
