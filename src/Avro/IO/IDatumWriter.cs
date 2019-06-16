namespace Avro.IO
{
    public interface IDatumWriter<T>
    {
        Schema WriterSchema { get; }
        void Write(IEncoder stream, T value);
    }
}
