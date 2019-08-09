using Avro.Schemas;
using System;

namespace Avro.Protocols
{
    public class ParameterSchema : IEquatable<ParameterSchema>
    {
        public ParameterSchema(string name, string type)
        {
            Name = name;
            Type = type;
        }
        public string Name { get; private set; }
        public string Type { get; private set; }

        public bool Equals(ParameterSchema other)
        {
            return other.Name == Name;
        }
    }
}
