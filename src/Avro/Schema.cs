using Avro.Schemas;
using System;
using System.IO;
using System.Text;

namespace Avro
{
    public abstract class Schema : IEquatable<Schema>
    {
        public Schema() { }

        public static Schema Parse(string text) => SchemaParser.Parse(text);

        public virtual bool Equals(Schema other)
        {
            return GetType() == other.GetType();
        }
    }
}
