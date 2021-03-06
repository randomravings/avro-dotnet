using Avro.Schema;
using System;
using System.Collections.Generic;

namespace Avro.Types
{
    public class GenericError : GenericRecord, IEquatable<GenericError>
    {
        public GenericError(AvroException exception, ErrorSchema schema)
            : base(schema)
        {
            Exception = exception;
        }

        public GenericError(AvroException exception, ErrorSchema schema, IReadOnlyDictionary<string, int> index, ValueTuple<int, Func<object>>[] defaultInitializers)
            : base(schema, index, defaultInitializers)
        {
            Exception = exception;
        }

        public GenericError(AvroException exception, GenericRecord record)
            : base(record, true)
        {
            Exception = exception;
        }

        public AvroException Exception { get; private set; }

        public bool Equals(GenericError other)
        {
            return
                base.Equals(other) &&
                Exception.Message.Equals(other.Exception.Message);
        }
    }
}
