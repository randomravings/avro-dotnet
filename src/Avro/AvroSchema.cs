using System;

namespace Avro
{
    public abstract class AvroSchema : AvroObject, IEquatable<AvroSchema>
    {
        public AvroSchema()
            : base() { }

        public virtual bool Equals(AvroSchema other)
        {
            return GetType().Equals(other.GetType());
        }

        public static AvroSchema Parse(string text) => AvroParser.ReadSchema(text);
    }
}
