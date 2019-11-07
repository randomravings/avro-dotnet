using Avro.Container;
using System;
using System.Collections.Generic;

namespace Avro.IO
{
    public interface IAvroFileReader<T> : IEnumerable<ReadBlock<T>>, IAsyncEnumerable<ReadBlock<T>>, IDisposable { }
}
