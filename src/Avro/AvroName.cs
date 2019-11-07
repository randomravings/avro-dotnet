using System;

namespace Avro
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    public sealed class AvroName : Attribute
    {
        public AvroName(string name)
        {
            Name = name;
        }
        public string Name { get; private set; }
    }
}
