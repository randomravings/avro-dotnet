using Avro.Utils;
using System;
using System.Collections.Generic;

namespace Avro.Schema
{
    public abstract class NamedSchema : AvroSchema, IEquatable<NamedSchema>
    {
        private string _name = string.Empty;
        private string _nameSpace = string.Empty;
        private IList<string> _aliases = new List<string>();

        public NamedSchema()
            : this(string.Empty, string.Empty) { }

        public NamedSchema(string name)
            : this(name, string.Empty) { }

        public NamedSchema(string name, string ns)
        {
            var items = name.Split('.');
            if (items.Length > 1)
            {
                Name = items[^1];
                Namespace = string.Join(".", items[0..^1]);
            }
            else
            {
                Name = name;
                Namespace = ns;
            }
        }

        public virtual string Name { get { return _name; } set { NameValidator.ValidateName(value); _name = value; } }
        public virtual string Namespace { get { return _nameSpace; } set { NameValidator.ValidateNamespace(value); _nameSpace = value; } }
        public string FullName => string.IsNullOrEmpty(Namespace) ? Name : $"{Namespace}.{Name}";
        public virtual IList<string> Aliases { get { return _aliases; } set { NameValidator.ValidateNames(value); _aliases = value; } }

        public override string ToString() => Name;

        public override bool Equals(object obj) => base.Equals(obj) && Equals((NamedSchema)obj);

        public bool Equals(NamedSchema other) => base.Equals(other) && Name == other.Name;

        public override int GetHashCode() => HashCode.Combine(base.GetHashCode(), Name);
    }
}
