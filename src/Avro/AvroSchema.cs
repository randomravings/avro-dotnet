using System;

namespace Avro
{
    public abstract class AvroSchema : AvroMeta
    {
        public AvroSchema()
        : base() { }

        public override bool Equals(object obj) => obj != null && GetType().Equals(obj.GetType());

        public override int GetHashCode() => base.GetHashCode();
    }
}
