namespace Avro.IO
{
    public interface IDatumWriter<T>
    {
        AvroSchema WriterSchema { get; }
        void Write(IEncoder stream, T value);
    }
}
