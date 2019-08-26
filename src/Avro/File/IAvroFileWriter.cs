using System;

namespace Avro.File
{
    public interface IAvroFileWriter<T> : IDisposable
    {
        void Write(T item);
    }
}
