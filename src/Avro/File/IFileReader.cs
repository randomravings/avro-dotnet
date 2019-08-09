using Avro.IO;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace Avro.File
{
    public interface IFileReader<T> : IEnumerable<T>, IDisposable { }
}
