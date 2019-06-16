using System;

namespace Avro.CodeDom
{
    public class CodeGenException : AvroException
    {
        public CodeGenException(string message)
            : base(message) { }
        public CodeGenException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}
