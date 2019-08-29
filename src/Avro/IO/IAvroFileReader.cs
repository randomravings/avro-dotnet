using System;
using System.Collections.Generic;

namespace Avro.IO
{
    public interface IAvroFileReader<T> : IEnumerable<T>, IDisposable { }
}
