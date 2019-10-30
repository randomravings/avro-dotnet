using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Schema
{
    public sealed class UnionSchema : AvroSchema, IList<AvroSchema>
    {
        private readonly IList<AvroSchema> _schemas;

        public UnionSchema(IEnumerable<AvroSchema> schemas)
        {
            _schemas = new List<AvroSchema>();
            if (schemas != null)
                foreach (var schema in schemas)
                    Add(schema);
        }

        public UnionSchema(params AvroSchema[] schemas)
            : this(schemas as IEnumerable<AvroSchema>) { }

        public int NullIndex => _schemas.IndexOf(new NullSchema());

        public override string ToString() => "union";

        public override void AddTag(string key, object value) => throw new NotSupportedException("Unions do not support tags");

        public override void AddTags(IEnumerable<KeyValuePair<string, object>> tags) => throw new NotSupportedException("Unions do not support tags");

        public override void RemoveTag(string key) => throw new NotSupportedException("Unions do not support tags");

        public IEnumerator<AvroSchema> GetEnumerator() => _schemas.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _schemas.GetEnumerator();

        public int Count => _schemas.Count;

        public bool IsReadOnly => throw new NotImplementedException();

        public AvroSchema this[int index] { get => _schemas[index]; set => _schemas[index] = CheckSchema(value); }

        public void Add(AvroSchema item) => _schemas.Add(CheckSchema(item));

        public void Clear() => _schemas.Clear();

        public int IndexOf(AvroSchema item) => _schemas.IndexOf(item);

        public void Insert(int index, AvroSchema item) => _schemas.Insert(index, item);

        public void RemoveAt(int index) => _schemas.RemoveAt(index);

        public bool Contains(AvroSchema item) => _schemas.Any(r => r.Equals(item));

        public void CopyTo(AvroSchema[] array, int arrayIndex) => _schemas.CopyTo(array, arrayIndex);

        public bool Remove(AvroSchema item) => _schemas.Remove(item);

        private AvroSchema CheckSchema(AvroSchema item) =>
            item switch
            {
                UnionSchema s => throw new AvroParseException($"Unions within unions is not supported."),
                ArraySchema s when _schemas.Any(r => r is ArraySchema) => throw new AvroParseException($"Union already contains an array schema."),
                MapSchema s when _schemas.Any(r => r is MapSchema) => throw new AvroParseException($"Union already contains a map schema."),
                NamedSchema s when _schemas.Any(r => r.Equals(s)) => throw new AvroParseException($"Unions already contains a schema with name: {s.Name}."),
                AvroSchema s when _schemas.Any(r => r.Equals(s)) => throw new AvroParseException($"Unions already contains a schema of: {item.ToString()}."),
                _ => item
            };
    }
}
