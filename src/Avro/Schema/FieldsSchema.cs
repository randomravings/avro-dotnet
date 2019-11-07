using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Schema
{
    public abstract class FieldsSchema : ComplexSchema, IList<FieldSchema>, IEquatable<FieldsSchema>
    {
        private readonly IDictionary<string, FieldSchema> _nameLookup = new Dictionary<string, FieldSchema>();
        private readonly IList<FieldSchema> _fields = new List<FieldSchema>();

        protected FieldsSchema()
            : base() { }

        protected FieldsSchema(string name)
            : base(name) { }

        protected FieldsSchema(string name, string ns)
            : base(name, ns) { }

        protected FieldsSchema(string name, IEnumerable<FieldSchema> fields)
            : base(name)
        {
            foreach (var field in fields)
                Add(field);
        }

        protected FieldsSchema(string name, string ns, IEnumerable<FieldSchema> fields)
            : base(name, ns)
        {
            foreach (var field in fields)
                Add(field);
        }

        public override string Namespace
        {
            get
            {
                return base.Namespace;
            }
            set
            {
                if (base.Namespace == value)
                    return;

                var old = base.Namespace;
                base.Namespace = value;

                var namedTypes =
                    _fields.Where(r => r.Type is NamedSchema)
                        .Select(t => (NamedSchema)t.Type)
                        .Where(r => r.Namespace == old || r.Namespace != null && r.Namespace.StartsWith(old))
                    ;

                foreach (var namedType in namedTypes)
                    if (string.IsNullOrEmpty(namedType.Namespace) || namedType.Namespace == old)
                        namedType.Namespace = value;
                    else
                        namedType.Namespace = $"{value}{namedType.Namespace.Remove(0, old.Length)}";
            }
        }

        public int Count => _fields.Count;

        public bool IsReadOnly => _fields.IsReadOnly;

        public FieldSchema this[int index] { get => _fields[index]; set => Insert(index, value); }

        public FieldSchema this[string name] { get => _nameLookup[name]; }

        public void Add(FieldSchema field)
        {
            _fields.Add(CheckField(field));
            _nameLookup.Add(field.Name, _fields.Last());
        }

        public bool Contains(string name) => _nameLookup.ContainsKey(name);

        public bool Remove(string name) =>
            _nameLookup.TryGetValue(name, out var field) switch
            {
                true => RemoveField(field),
                _ => false
            };

        private bool RemoveField(FieldSchema item) =>
            _nameLookup.Remove(item.Name) switch
            {
                true => _fields.Remove(item),
                _ => false
            };

        public IEnumerator<FieldSchema> GetEnumerator() => _fields.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _fields.GetEnumerator();

        public int IndexOf(FieldSchema item) => _fields.IndexOf(item);

        public int IndexOf(string name) =>
            _nameLookup.TryGetValue(name, out var item) switch
            {
                true => _fields.IndexOf(item),
                _ => -1
            };

        public void Insert(int index, FieldSchema item) => _fields.Insert(index, CheckField(item));

        public void RemoveAt(int index) => RemoveField(_fields[index]);

        public void Clear()
        {
            _nameLookup.Clear();
            _fields.Clear();
        }

        public bool Contains(FieldSchema item) => _fields.Contains(item);

        public void CopyTo(FieldSchema[] array, int arrayIndex) => _fields.CopyTo(array, arrayIndex);

        public bool Remove(FieldSchema item) =>
            _fields.Remove(item) switch
            {
                true => _nameLookup.Remove(item.Name),
                _ => false
            };

        private FieldSchema CheckField(FieldSchema item) =>
            item switch
            {
                var x when x.Name == string.Empty => throw new AvroParseException("Unnamed fields are not supported."),
                var x when Contains(x.Name) => throw new AvroParseException($"Record already contains field with name: '{item.Name}'."),
                var x when x.Type is NamedSchema && ((NamedSchema)x.Type).Namespace == string.Empty => SetChildNamespace(x),
                _ => item
            };

        private FieldSchema SetChildNamespace(FieldSchema item)
        {
            ((NamedSchema)item.Type).Namespace = Namespace;
            return item;
        }

        public bool Equals(FieldsSchema other) => other != null && Name == other.Name;
    }
}
