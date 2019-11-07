using System;

namespace Avro.IO
{
    public interface IAvroFileWriter<T> : IDisposable
    {
        void Write(T item);
        void Flush();
        void Close();
    }
}
