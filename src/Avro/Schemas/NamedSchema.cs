using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Avro.Schemas
{
    public abstract class NamedSchema : Schema
    {
        protected static readonly Regex NAME_VALIDATOR = new Regex("[A-Za-z_][A-Za-z0-9_]*");

        private string _name;
        private string _nameSpace;
        private IList<string> _aliases;

        public NamedSchema()
        {
            Aliases = new List<string>();
        }

        public NamedSchema(string name)
        {
            var items = name.Split('.');
            if(items.Length > 1)
            {
                Name = items.Last();
                Namespace = string.Join(".", items.Take(items.Length - 1));
            }
            else
            {
                Name = name;
                Namespace = null;
            }
            Aliases = new List<string>();
        }

        public NamedSchema(string name, string ns)
        {
            var items = name.Split('.');
            if (items.Length > 1)
            {
                Name = items.Last();
                Namespace = string.Join(".", items.Take(items.Length - 1));
            }
            else
            {
                Name = name;
                Namespace = ns;
            }
            Aliases = new List<string>();
        }

        public string Name { get { return _name; } set { ValidateName(value); _name = value; } }
        public string Namespace { get { return _nameSpace; } set { ValidateNameSpace(value); _nameSpace = value; } }
        public string FullName => string.IsNullOrEmpty(Namespace) ? Name : $"{Namespace}.{Name}";
        public IList<string> Aliases { get { return _aliases; } set { if (value != null) ValidateAliases(value); _aliases = value; } }

        public override string ToString() => FullName;

        public override bool Equals(Schema other)
        {
            return (other is NamedSchema) && (other as NamedSchema).FullName == FullName;
        }

        protected static void ValidateName(string name)
        {
            var match = NAME_VALIDATOR.Match(name);
            if (name != match.Value)
                throw new SchemaParseException($"Name must match the regex {NAME_VALIDATOR.ToString()}");
        }

        protected static void ValidateNameSpace(string ns)
        {
            if (ns == null)
                return;
            var components = ns.Split('.');
            foreach (var component in components)
            {
                var match = NAME_VALIDATOR.Match(component);
                if (component != match.Value)
                    throw new SchemaParseException($"Namespace components must match the regex {NAME_VALIDATOR.ToString()}");
            }
        }

        protected static void ValidateAliases(IEnumerable<string> aliases)
        {
            foreach (var alias in aliases)
            {
                var match = NAME_VALIDATOR.Match(alias);
                if (alias != match.Value)
                    throw new SchemaParseException($"Alias must match the regex {NAME_VALIDATOR.ToString()}");
            }
        }
    }
}
