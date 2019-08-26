using Avro.Schemas;
using System;
using System.Collections.Generic;

namespace Avro.Types
{
    public class GenericAvroError : GenericAvroRecord, IEquatable<GenericAvroError>
    {
        public GenericAvroError(AvroException exception, ErrorSchema schema)
            : base(schema)
        {
            Exception = exception;
        }

        public GenericAvroError(AvroException exception, ErrorSchema schema, IReadOnlyDictionary<string, int> index, ValueTuple<int, Func<object>>[] defaultInitializers)
            : base(schema, index, defaultInitializers)
        {
            Exception = exception;
        }

        public GenericAvroError(AvroException exception, GenericAvroRecord record)
            : base(record, true)
        {
            Exception = exception;
        }

        public AvroException Exception { get; private set; }

        public bool Equals(GenericAvroError other)
        {
            return
                base.Equals(other) &&
                Exception.Message.Equals(other.Exception.Message);
        }
    }
}
