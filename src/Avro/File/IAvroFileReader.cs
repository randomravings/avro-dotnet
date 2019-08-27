using System;
using System.Collections.Generic;

namespace Avro.File
{
    public interface IAvroFileReader<T> : IEnumerable<T>, IDisposable { }
}
