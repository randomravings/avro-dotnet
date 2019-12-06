using Avro.Types;

namespace Avro.Protocol
{
    public sealed class GenericResponse : AvroUnion<GenericRecord, GenericFixed, GenericEnum>
    {
        private GenericResponse() : base(GenericRecord.Empty) { }
        public GenericResponse(GenericRecord value) : base(value) { }
        public GenericResponse(GenericFixed value) : base(value) { }
        public GenericResponse(GenericEnum value) : base(value) { }
        public static GenericResponse Empty { get; } = new GenericResponse();
        public static implicit operator GenericRecord(GenericResponse union) => union.GetT1();
        public static implicit operator GenericFixed(GenericResponse union) => union.GetT2();
        public static implicit operator GenericEnum(GenericResponse union) => union.GetT3();
        public static implicit operator GenericResponse(GenericRecord value) => new GenericResponse(value);
        public static implicit operator GenericResponse(GenericFixed value) => new GenericResponse(value);
        public static implicit operator GenericResponse(GenericEnum value) => new GenericResponse(value);
    }
}
