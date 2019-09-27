using Avro.Serialization;
using Avro.Types;
using Avro.Utils;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Schema
{
    [SerializationType(typeof(GenericRecord), CompatibleTypes = new [] { typeof(IAvroRecord) })]
    public class RecordSchema : ComplexSchema, IEnumerable<RecordSchema.Field>
    {
        private readonly IList<Field> _fields = new List<Field>();

        public RecordSchema()
            : base() { }

        public RecordSchema(string name)
            : base(name) { }

        public RecordSchema(string name, string ns)
            : base(name, ns) { }

        public RecordSchema(string name, IEnumerable<Field> fields)
            : base(name)
        {
            foreach (var field in fields)
                Add(field);
        }

        public RecordSchema(string name, string ns, IEnumerable<Field> fields)
            : base(name, ns)
        {
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
            if (field.Type is NamedSchema && string.IsNullOrEmpty((field.Type as NamedSchema).Namespace))
                (field.Type as NamedSchema).Namespace = Namespace;
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
                        .Select(t => t.Type as NamedSchema)
                        .Where(r => r.Namespace == old || r.Namespace != null && r.Namespace.StartsWith(old))
                    ;

                foreach (var namedType in namedTypes)
                    if (string.IsNullOrEmpty(namedType.Namespace) || namedType.Namespace == old)
                        namedType.Namespace = value;
                    else
                        namedType.Namespace = $"{value}{namedType.Namespace.Remove(0, old.Length)}";
            }
        }

        public IEnumerator<Field> GetEnumerator() => _fields.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _fields.GetEnumerator();

        public class Field : AvroSchema, IEquatable<Field>
        {
            private string _name;
            private IList<string> _aliases;
            private JToken _default;

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

            public Field(string name, AvroSchema type)
                : base()
            {
                Name = name;
                Type = type;
                Aliases = new List<string>();
                Default = null;
            }

            public string Name { get { return _name; } set { NameValidator.ValidateName(value); _name = value; } }

            public AvroSchema Type { get; set; }

            public string Order { get; set; }
            public JToken Default { get { return _default; } set { DefaultValidator.ValidateJson(Type, value); _default = value; } }
            public string Doc { get; set; }
            public IList<string> Aliases { get { return _aliases; } set { NameValidator.ValidateNames(value); _aliases = value; } }

            public bool Equals(Field other)
            {
                return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);
            }
        }
    }
}
