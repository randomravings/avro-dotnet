using System.Collections;
using System.Collections.Generic;

namespace Avro.Schemas
{
    public class UnionSchema : Schema, IList<Schema>
    {
        private readonly IList<Schema> _types;

        public UnionSchema()
        {
            _types = new List<Schema>();
        }

        public UnionSchema(IEnumerable<Schema> schemas)
            : this()
        {
            foreach (var schema in schemas)
                Add(schema);
        }

        public UnionSchema(params Schema[] schemas)
            : this()
        {
            if (schemas != null)
                foreach (var schema in schemas)
                    Add(schema);
        }

        private void ValidateType(Schema item)
        {
            if (item is UnionSchema)
                throw new SchemaParseException($"Unions within unions is not supported.");
            if (Contains(item))
                throw new SchemaParseException($"Union already contains schema.");
        }

        public void Add(Schema item)
        {
            ValidateType(item);
            _types.Add(item);
        }

        public override string ToString() => "union";

        public IEnumerator<Schema> GetEnumerator() => _types.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _types.GetEnumerator();

        public int Count => _types.Count;

        public bool IsReadOnly => _types.IsReadOnly;

        Schema IList<Schema>.this[int index] { get { return _types[index]; } set { ValidateType(value); _types[index] = value; } }

        public int IndexOf(Schema item) => _types.IndexOf(item);

        public void Insert(int index, Schema item)
        {
            ValidateType(item);
            _types.Insert(index, item);
        }

        public void RemoveAt(int index) => _types.RemoveAt(index);

        public void Clear() => _types.Clear();

        public bool Contains(Schema item) => _types.Contains(item);

        public void CopyTo(Schema[] array, int arrayIndex) => _types.CopyTo(array, arrayIndex);

        public bool Remove(Schema item) => _types.Remove(item);

        public Schema this[int index] { get { return _types[index]; } set { ValidateType(value); _types[index] = value; } }
    }
}
