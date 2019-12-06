using Avro.Schema;
using Avro.Types;
using System;

namespace Avro.Protocol
{
    public sealed class GenericResponseError : GenericUnion<GenericResponseError>
    {
        private GenericResponseError() : base() { }
        public GenericResponseError(GenericResponseError error)
            : base(error) { }
        public GenericResponseError(UnionSchema schema, params Type[] types)
            : base(schema, types, string.Empty)
        {
            if (schema.Count < 1 || !(schema[0] is StringSchema))
                throw new ArgumentException($"{nameof(schema)}[0] must be of type '{nameof(StringSchema)}'");
            for(int i = 1; i < schema.Count; i++)
                if(!(schema[i] is ErrorSchema))
                    throw new ArgumentException($"{nameof(schema)}[1..n] must be of type '{nameof(GenericError)}'");
        }
        public static GenericResponseError Empty { get; } = new GenericResponseError();
        public static implicit operator string(GenericResponseError e) => e.Get<string>(e._index);
        public static implicit operator GenericError(GenericResponseError e) => e.Get<GenericError>(e._index);
        protected override GenericResponseError New() => new GenericResponseError(this);
    }
}
