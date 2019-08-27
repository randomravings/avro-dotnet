namespace Avro.IO
{
    public interface IAvroWriter<T>
    {
        AvroSchema WriterSchema { get; }
        void Write(IAvroEncoder stream, T value);
    }
}
