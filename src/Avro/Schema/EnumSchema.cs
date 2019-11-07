using Avro.Utils;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Schema
{
    public sealed class EnumSchema : ComplexSchema, IReadOnlyDictionary<string, int>
    {
        private readonly IList<string> _symbols;

        public EnumSchema()
            : base()
        {
            _symbols = new List<string>();
        }

        public EnumSchema(string name)
            : base(name)
        {
            _symbols = new List<string>();
        }

        public EnumSchema(string name, string ns)
            : base(name, ns)
        {
            _symbols = new List<string>();
        }

        public EnumSchema(string name, string ns, IEnumerable<string> symbols)
            : base(name, ns)
        {
            NameValidator.ValidateSymbols(symbols);
            _symbols = new List<string>(symbols);
        }

        public string this[int i] => _symbols[i];

        public int this[string key]
        {
            get
            {
                var i = _symbols.IndexOf(key);
                if (i == -1)
                    throw new KeyNotFoundException($"Enum does not contain symbol: '{key}'");
                return i;
            }
        }

        public IEnumerable<string> Keys => _symbols;

        public IEnumerable<int> Values => Enumerable.Range(0, _symbols.Count);

        public int Count => _symbols.Count;

        public bool ContainsKey(string key) => _symbols.Contains(key);

        public IEnumerator<KeyValuePair<string, int>> GetEnumerator()
        {
            for (int i = 0; i < _symbols.Count; i++)
                yield return new KeyValuePair<string, int>(_symbols[i], i);
        }

        public bool TryGetValue(string key, out int value)
        {
            value = _symbols.IndexOf(key);
            return value != -1;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
