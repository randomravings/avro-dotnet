using Avro.Types;

namespace Avro.Protocol
{
    public sealed class GenericResponseError : AvroUnion<string, GenericError>
    {
        public GenericResponseError() : base(string.Empty) { }
        public GenericResponseError(string value) : base(value) { }
        public GenericResponseError(GenericError value) : base(value) { }
        public static implicit operator string(GenericResponseError union) => union.GetT1();
        public static implicit operator GenericError(GenericResponseError union) => union.GetT2();
        public static implicit operator GenericResponseError(string value) => new GenericResponseError(value);
        public static implicit operator GenericResponseError(GenericError value) => new GenericResponseError(value);
    }
}
