namespace Avro.IO
{
    public interface IAvroReader<T>
    {
        AvroSchema ReaderSchema { get; }
        AvroSchema WriterSchema { get; }
        T Read(IAvroDecoder stream);
        T Read(IAvroDecoder stream, ref T reuse);
        void Skip(IAvroDecoder stream);
    }
}
