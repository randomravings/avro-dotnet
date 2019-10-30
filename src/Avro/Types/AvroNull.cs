namespace Avro.Types
{
    public sealed class AvroNull
    {
        private AvroNull() { }
        public static AvroNull Value { get; } = new AvroNull();
        public override bool Equals(object obj) => obj != null && obj is AvroNull;
        public override int GetHashCode() => base.GetHashCode();
        public override string ToString() => nameof(AvroNull);
    }
}
