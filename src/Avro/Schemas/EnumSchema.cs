using Avro.Utils;
using System.Collections.Generic;

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
            NameValidator.ValidateSymbols(symbols);
            Symbols = new List<string>(symbols);
        }

        public IList<string> Symbols { get { return _symbols; } set { NameValidator.ValidateSymbols(value); _symbols = value; } }
    }
}
