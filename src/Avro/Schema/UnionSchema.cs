using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Schema
{
    public class UnionSchema : AvroSchema, IEnumerable<AvroSchema>
    {
        private readonly IList<AvroSchema> _types;

        public UnionSchema()
        {
            _types = new List<AvroSchema>();
        }

        public UnionSchema(params AvroSchema[] schemas)
            : this()
        {
            if (schemas != null)
                foreach (var schema in schemas)
                    Add(schema);
        }

        private void ValidateType(AvroSchema item)
        {
            switch (item)
            {
                case UnionSchema s:
                    throw new AvroParseException($"Unions within unions is not supported.");
                case ArraySchema s when _types.FirstOrDefault(r => r.GetType().Equals(s.GetType())) != null:
                    throw new AvroParseException($"Union already contains an array schema.");
                case MapSchema s when _types.FirstOrDefault(r => r.GetType().Equals(s.GetType())) != null:
                    throw new AvroParseException($"Union already contains a map schema.");
                case NamedSchema s when _types.Contains(item):
                    throw new AvroParseException($"Unions already contains a schema with name: {s.FullName}.");
                default:
                    if (_types.Contains(item))
                        throw new AvroParseException($"Unions already contains a schema of: {item.ToString()}.");
                    break;
            }
        }

        public void Add(AvroSchema item)
        {
            ValidateType(item);
            _types.Add(item);
        }

        public override string ToString() => "union";

        public override void AddTag(string key, object value) => throw new NotSupportedException("Unions do not support tags");
        public override void AddTags(IEnumerable<KeyValuePair<string, object>> tags) => throw new NotSupportedException("Unions do not support tags");
        public override void RemoveTag(string key) => throw new NotSupportedException("Unions do not support tags");

        public IEnumerator<AvroSchema> GetEnumerator() => _types.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _types.GetEnumerator();

        public int Count => _types.Count;

        public void Clear() => _types.Clear();

        public AvroSchema this[int index] { get { return _types[index]; } }
    }
}
