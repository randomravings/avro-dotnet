using Avro.IO;
using System;

namespace Avro.Generic
{
    public struct GenericFieldReader
    {
        public GenericFieldReader(int fieldPos, Tuple<Func<IDecoder, object>, Action<IDecoder>> reader)
        {
            FieldPos = fieldPos;
            Reader = reader;
        }
        public int FieldPos { get; private set; }
        public Tuple<Func<IDecoder, object>, Action<IDecoder>> Reader { get; private set; }
    }
}
