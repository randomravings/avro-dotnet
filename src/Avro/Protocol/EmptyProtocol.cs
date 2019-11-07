using System;
using System.Collections.Generic;

namespace Avro.Protocol
{
    public class EmptyProtocol : AvroProtocol
    {
        private EmptyProtocol()
            : base() { }
        public static EmptyProtocol Value { get; } = new EmptyProtocol();
        public override void AddTag(string key, object value) => throw new NotSupportedException();
        public override void AddTags(IEnumerable<KeyValuePair<string, object>> tags) => throw new NotSupportedException();
        public override void RemoveTag(string key) => throw new NotSupportedException();
        public override string ToString() => "empty";
    }
}
