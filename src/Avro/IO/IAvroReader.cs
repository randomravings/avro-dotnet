namespace Avro.IO
{
    public interface IAvroReader<T>
    {
        AvroSchema ReaderSchema { get; }
        AvroSchema WriterSchema { get; }
        T Read(IAvroDecoder stream);
        void Skip(IAvroDecoder stream);
    }
}
