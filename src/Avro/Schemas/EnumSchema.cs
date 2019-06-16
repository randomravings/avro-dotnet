using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Avro.Schemas
{
    public class EnumSchema : ComplexSchema
    {
        private IList<string> _symbols;

        public EnumSchema()
            : base()
        {
            Symbols = new List<string>();
        }

        public EnumSchema(string name)
            : base(name)
        {
            Symbols = new List<string>();
        }

        public EnumSchema(string name, string ns)
            : base(name, ns)
        {
            Symbols = new List<string>();
        }

        public EnumSchema(string name, string ns, IEnumerable<string> symbols)
            : base(name, ns)
        {
            ValidateSymbols(symbols);
            Symbols = new List<string>(symbols);
        }

        public IList<string> Symbols { get { return _symbols; } set { ValidateSymbols(value); _symbols = value; } }

        protected static void ValidateSymbols(IEnumerable<string> symbols)
        {
            foreach (var symbol in symbols)
            {
                var match = NAME_VALIDATOR.Match(symbol);
                if (symbol != match.Value)
                    throw new SchemaParseException($"Symbols must match the regex {NAME_VALIDATOR.ToString()}");
            }

            var duplicates = symbols.GroupBy(r => r).Where(g => g.Count() > 1).Select(k => k.Key);
            if(duplicates.Count() > 0)
                throw new SchemaParseException($"Duplicate symbols: {string.Join(",", duplicates)}");
        }
    }
}
