using Avro.Serialization;
using Avro.Types;
using System.Collections.Generic;

namespace Avro.Schema
{
    [SerializationType(typeof(GenericError), CompatibleTypes = new [] { typeof(IAvroError) })]
    public class ErrorSchema : RecordSchema
    {
        public ErrorSchema()
            : base() { }

        public ErrorSchema(string name)
            : base(name) { }

        public ErrorSchema(string name, string ns)
            : base(name, ns) { }

        public ErrorSchema(string name, IEnumerable<Field> fields)
            : base(name, fields) { }

        public ErrorSchema(string name, string ns, IEnumerable<Field> fields)
            : base(name, ns, fields) { }
    }
}
