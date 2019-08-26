using Avro.Utils;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Schemas
{
    public abstract class NamedSchema : AvroSchema
    {
        private string _name;
        private string _nameSpace;
        private IList<string> _aliases;

        public NamedSchema()
            : this(null, null) { }

        public NamedSchema(string name)
            : this(name, null) { }

        public NamedSchema(string name, string ns)
        {
            var items = name?.Split('.') ?? new string[0];
            if (items.Length > 1 && ns == null)
            {
                Name = items.Last();
                Namespace = string.Join(".", items.Take(items.Length - 1));
            }
            else
            {
                if(name != null)
                    Name = name;
                Namespace = ns;
            }
            Aliases = new List<string>();
        }

        public string Name { get { return _name; } set { NameValidator.ValidateName(value); _name = value; } }
        public string Namespace { get { return _nameSpace; } set { NameValidator.ValidateNamespace(value); _nameSpace = value; } }
        public string FullName => string.IsNullOrEmpty(Namespace) ? Name : $"{Namespace}.{Name}";
        public IList<string> Aliases { get { return _aliases; } set { if (value != null) NameValidator.ValidateNames(value); _aliases = value; } }

        public override string ToString() => FullName;

        public override bool Equals(AvroSchema other)
        {
            return (other is NamedSchema) &&
                (other as NamedSchema).FullName == FullName
            ;
        }
    }
}
