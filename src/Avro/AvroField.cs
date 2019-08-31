using System;

namespace Avro
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    public sealed class AvroField : Attribute
    {
        public AvroField(string name)
        {
            Name = name;
        }
        public string Name { get; private set; }
    }
}
