namespace Avro.IO
{
    public interface IDatumReader<T>
    {
        Schema ReaderSchema { get; }
        Schema WriterSchema { get; }
        T Read(IDecoder stream);
        T Read(IDecoder stream, ref T reuse);
        void Skip(IDecoder stream);
    }
}
