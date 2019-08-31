using Avro.Schema;
using System;

namespace Avro.Protocol.Schema
{
    public class ParameterSchema : IEquatable<ParameterSchema>
    {
        public ParameterSchema(string name, NamedSchema type)
        {
            Name = name;
            Type = type;
        }
        public string Name { get; private set; }
        public NamedSchema Type { get; private set; }

        public bool Equals(ParameterSchema other)
        {
            return other.Name == Name;
        }
    }
}
