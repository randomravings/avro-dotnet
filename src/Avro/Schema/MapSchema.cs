using System;

namespace Avro.Schema
{
    public sealed class MapSchema : AvroSchema, IEquatable<MapSchema>
    {
        public MapSchema(AvroSchema values)
        {
            Values = values;
        }

        public AvroSchema Values { get; set; }

        public override bool Equals(object other) => base.Equals(other) && Equals((MapSchema)other);

        public override int GetHashCode() => HashCode.Combine(base.GetHashCode(), Values);

        public override string ToString() => "map";

        public bool Equals(MapSchema other) => other != null && Values.Equals(other.Values);
    }
}
