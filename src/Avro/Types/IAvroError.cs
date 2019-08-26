namespace Avro.Types
{
    public interface IAvroError : IAvroRecord
    {
        AvroException Exception { get; }
}
}
