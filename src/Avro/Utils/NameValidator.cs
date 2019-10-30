using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Avro.Utils
{
    /// <summary>
    /// Used to validate name components in Avro schemas.
    /// </summary>
    public static class NameValidator
    {
        private static readonly Regex NAME_VALIDATOR = new Regex("[A-Za-z_][A-Za-z0-9_]*");

        /// <summary>
        /// Validates a single named component.
        /// </summary>
        /// <param name="name">Name to validate.</param>
        /// <exception cref="AvroParseException">If one name contains bad character.</exception>
        public static void ValidateName(string name)
        {
            if (name == string.Empty)
                return;
            var match = NAME_VALIDATOR.Match(name);
            if (!match.Success || name != match.Value)
                throw new AvroParseException($"Name must match the regex {NAME_VALIDATOR.ToString()}");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ns">Qualified namespace.</param>
        /// <exception cref="AvroParseException">If one name contains bad character.</exception>
        public static void ValidateNamespace(string ns)
        {
            if (ns == string.Empty)
                return;
            var components = ns.Split('.');
            ValidateNames(components);
        }

        /// <summary>
        /// Validates a set of names.
        /// </summary>
        /// <param name="names">List of names.</param>
        /// <exception cref="AvroParseException">If one name contains bad character.</exception>
        public static void ValidateNames(IEnumerable<string> names)
        {
            foreach (var name in names)
                ValidateName(name);
        }

        /// <summary>
        /// Validates Enum Symbols.
        /// </summary>
        /// <param name="symbols">List of symbols.</param>
        /// <exception cref="AvroParseException">If a symbol contains bad characters or duplicates symbols are identified.</exception>
        public static void ValidateSymbols(IEnumerable<string> symbols)
        {
            ValidateNames(symbols);
            var duplicates = symbols.GroupBy(r => r).Where(g => g.Count() > 1).Select(k => k.Key);
            if (duplicates.Count() > 0)
                throw new AvroParseException($"Duplicate symbols: {string.Join(",", duplicates)}");
        }
    }
}
