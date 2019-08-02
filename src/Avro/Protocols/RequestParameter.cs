using Avro.Schemas;
using System;

namespace Avro.Protocols
{
    public class RequestParameter : IEquatable<RequestParameter>
    {
        public RequestParameter(string name, RecordSchema type)
        {
            Name = name;
            Type = type;
        }
        public string Name { get; private set; }
        public RecordSchema Type { get; private set; }

        public bool Equals(RequestParameter other)
        {
            return other.Name == Name;
        }
    }
}
