using System.Collections.Generic;

namespace Avro.Schema
{
    public class ErrorSchema : RecordSchema
    {
        public ErrorSchema()
            : base() { }

        public ErrorSchema(string name)
            : base(name) { }

        public ErrorSchema(string name, string ns)
            : base(name, ns) { }

        public ErrorSchema(string name, IEnumerable<RecordFieldSchema> fields)
            : base(name, fields) { }

        public ErrorSchema(string name, string ns, IEnumerable<RecordFieldSchema> fields)
            : base(name, ns, fields) { }
    }
}
