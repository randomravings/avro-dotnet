using System;
using System.Collections.Generic;

namespace Avro.Schema
{
    public sealed class RecordSchema : FieldsSchema, IEquatable<RecordSchema>
    {
        public RecordSchema()
            : base() { }

        public RecordSchema(string name)
            : base(name) { }

        public RecordSchema(string name, string ns)
            : base(name, ns) { }

        public RecordSchema(string name, IEnumerable<FieldSchema> fields)
            : base(name, fields) { }

        public RecordSchema(string name, string ns, IEnumerable<FieldSchema> fields)
            : base(name, ns, fields) { }

        public bool Equals(RecordSchema other) => base.Equals(other);
    }
}
