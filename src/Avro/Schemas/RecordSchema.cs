using Avro.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Schemas
{
    public class RecordSchema : ComplexSchema, IEnumerable<RecordSchema.Field>
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

        public void Add(Field field)
        {
            if (field == null || string.IsNullOrEmpty(field.Name))
                throw new AvroParseException("Null or unnamed fields are not supported.");
            if (Contains(field.Name))
                throw new AvroParseException($"Record already contains field with name: '{field.Name}'.");
            _fields.Add(field);
        }

        public bool Contains(string name)
        {
            return _fields.Any(r => string.Equals(r.Name, name, StringComparison.OrdinalIgnoreCase));
        }

        public bool Remove(string name)
        {
            var field = _fields.FirstOrDefault(r => string.Equals(r.Name, name, StringComparison.OrdinalIgnoreCase));
            if (field != null)
                return _fields.Remove(field);
            return false;
        }

        public IEnumerator<Field> GetEnumerator() => _fields.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _fields.GetEnumerator();

        public class Field : AvroObject, IEquatable<Field>
        {
            private string _name;
            private IList<string> _aliases;

            public Field()
                : base()
            {
                Aliases = new List<string>();
            }

            public Field(string name)
                : base()
            {
                Name = name;
                Aliases = new List<string>();
            }

            public Field(string name, Schema type)
                : base()
            {
                Name = name;
                Type = type;
                Aliases = new List<string>();
            }

            public string Name { get { return _name; } set { NameValidator.ValidateName(value); _name = value; } }

            public Schema Type { get; set; }

            public string Order { get; set; }
            public object Default { get; set; }
            public string Doc { get; set; }
            public IList<string> Aliases { get { return _aliases; } set { NameValidator.ValidateNames(value); _aliases = value; } }

            public bool Equals(Field other)
            {
                return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);
            }
        }
    }
}
