using System;
using System.Collections;
using System.Collections.Generic;

namespace Avro.Schemas
{
    public class RecordSchema : ComplexSchema, ICollection<RecordSchema.Field>
    {
        private readonly IList<Field> _fields;

        public RecordSchema()
            : base()
        {
            _fields = new List<Field>();
        }

        public RecordSchema(string name)
            : base(name)
        {
            _fields = new List<Field>();
        }

        public RecordSchema(string name, string ns)
            : base(name, ns)
        {
            _fields = new List<Field>();
        }

        public RecordSchema(IEnumerable<Field> fields)
            : base()
        {
            _fields = new List<Field>();
            foreach (var field in fields)
                Add(field);
        }

        public RecordSchema(string name, IEnumerable<Field> fields)
            : base(name)
        {
            _fields = new List<Field>();
            foreach (var field in fields)
                Add(field);
        }

        public RecordSchema(string name, string ns, IEnumerable<Field> fields)
            : base(name, ns)
        {
            _fields = new List<Field>();
            foreach (var field in fields)
                Add(field);
        }

        public int Count => _fields.Count;

        public bool IsReadOnly => _fields.IsReadOnly;

        public void Add(Field item)
        {
            if (item == null ||string.IsNullOrEmpty(item.Name))
                throw new SchemaParseException("Null or unnamed fields are not supported.");
            if (Contains(item))
                throw new SchemaParseException($"Record already contains field with name: '{item.Name}'.");
            _fields.Add(item);
        }

        public void Clear() => _fields.Clear();

        public bool Contains(Field item) => _fields.Contains(item);

        public void CopyTo(Field[] array, int arrayIndex) => _fields.CopyTo(array, arrayIndex);

        public IEnumerator<Field> GetEnumerator() => _fields.GetEnumerator();

        public bool Remove(Field item) => _fields.Remove(item);

        IEnumerator IEnumerable.GetEnumerator() => _fields.GetEnumerator();

        public class Field : IEquatable<Field>
        {
            private string _name;
            private IList<string> _aliases;

            public Field()
            {
                Aliases = new List<string>();
            }

            public Field(string name)
            {
                Name = name;
                Aliases = new List<string>();
            }

            public Field(string name, Schema type)
            {
                Name = name;
                Type = type;
                Aliases = new List<string>();
            }

            public string Name { get { return _name; } set { ValidateName(value); _name = value; } }

            public Schema Type { get; set; }

            public string Order { get; set; }
            public object Default { get; set; }
            public string Doc { get; set; }
            public IList<string> Aliases { get { return _aliases; } set { ValidateAliases(value); _aliases = value; } }

            public bool Equals(Field other) => Name == other.Name;
        }
    }
}
