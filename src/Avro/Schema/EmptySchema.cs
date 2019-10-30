using System;
using System.Collections.Generic;

namespace Avro.Schema
{
    public sealed class EmptySchema : AvroSchema
    {
        private EmptySchema()
            : base() { }
        public static EmptySchema Value { get; } = new EmptySchema();
        public override void AddTag(string key, object value) => throw new NotSupportedException();
        public override void AddTags(IEnumerable<KeyValuePair<string, object>> tags) => throw new NotSupportedException();
        public override void RemoveTag(string key) => throw new NotSupportedException();
        public override string ToString() => "empty";
    }
}
