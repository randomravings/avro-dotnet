using System;

namespace Avro.Schemas
{
    public class SchemaParseException : Exception
    {
        public SchemaParseException(string s)
            : base(s) { }

        public SchemaParseException(string s, Exception innerException)
            : base(s, innerException) { }
    }
}
