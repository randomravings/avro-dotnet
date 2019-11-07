using System;
using System.Collections.Generic;

namespace Avro.Schema
{
    public sealed class ErrorSchema : FieldsSchema, IEquatable<ErrorSchema>
    {
        public ErrorSchema()
            : base() { }

        public ErrorSchema(string name)
            : base(name) { }

        public ErrorSchema(string name, string ns)
            : base(name, ns) { }

        public ErrorSchema(string name, IEnumerable<FieldSchema> fields)
            : base(name, fields) { }

        public ErrorSchema(string name, string ns, IEnumerable<FieldSchema> fields)
            : base(name, ns, fields) { }

        public bool Equals(ErrorSchema other) => base.Equals(other);
    }
}
