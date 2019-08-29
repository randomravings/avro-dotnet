using System;

namespace Avro
{
    public abstract class AvroSchema : AvroMeta, IEquatable<AvroSchema>
    {
        public AvroSchema()
        : base() { }

        public virtual bool Equals(AvroSchema other)
        {
            return GetType().Equals(other.GetType());
        }
    }
}
