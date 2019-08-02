using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Avro.Utils
{
    public static class NameValidator
    {
        private static readonly Regex NAME_VALIDATOR = new Regex("[A-Za-z_][A-Za-z0-9_]*");

        public static void ValidateName(string name)
        {
            var match = NAME_VALIDATOR.Match(name);
            if (!match.Success || name != match.Value)
                throw new AvroParseException($"Name must match the regex {NAME_VALIDATOR.ToString()}");
        }

        public static void ValidateNamespace(string ns)
        {
            if (ns == null)
                return;
            var components = ns.Split('.');
            ValidateNames(components);
        }

        public static void ValidateNames(IEnumerable<string> names)
        {
            foreach (var name in names)
                ValidateName(name);
        }

        public static void ValidateSymbols(IEnumerable<string> symbols)
        {
            ValidateNames(symbols);
            var duplicates = symbols.GroupBy(r => r).Where(g => g.Count() > 1).Select(k => k.Key);
            if (duplicates.Count() > 0)
                throw new AvroParseException($"Duplicate symbols: {string.Join(",", duplicates)}");
        }
    }
}
