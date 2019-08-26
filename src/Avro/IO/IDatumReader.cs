namespace Avro.IO
{
    public interface IDatumReader<T>
    {
        AvroSchema ReaderSchema { get; }
        AvroSchema WriterSchema { get; }
        T Read(IDecoder stream);
        T Read(IDecoder stream, ref T reuse);
        void Skip(IDecoder stream);
    }
}
