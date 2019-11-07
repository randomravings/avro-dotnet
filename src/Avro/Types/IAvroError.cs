using Avro.Schema;

namespace Avro.Types
{
    public interface IAvroError
    {
        ErrorSchema Schema { get; }
        int FieldCount { get; }
        object? this[int i] { get; set; }
}
}
