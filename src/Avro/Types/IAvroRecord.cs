using Avro.Schema;

namespace Avro.Types
{
    public interface IAvroRecord
    {
        RecordSchema Schema { get; }
        int FieldCount { get; }
        object this[int i] { get; set; }
    }
}
