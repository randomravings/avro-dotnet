using System;

namespace Avro
{
    public abstract class Schema : AvroObject, IEquatable<Schema>
    {
        public Schema()
            : base() { }

        public virtual bool Equals(Schema other)
        {
            return GetType() == other.GetType();
        }

        public static Schema Parse(string text) => AvroReader.ReadSchema(text);
    }
}
