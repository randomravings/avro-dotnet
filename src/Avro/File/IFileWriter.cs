using System;

namespace Avro.File
{
    public interface IFileWriter<T> : IDisposable
    {
        void Write(T item);
    }
}
