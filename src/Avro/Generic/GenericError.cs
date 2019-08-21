using System;

namespace Avro.Generic
{
    public sealed class GenericError : AvroException,  IEquatable<GenericError>
    {
        public GenericError(GenericRecord record)
            : base(record.Schema.FullName)
        {
            AvroData = record;
        }
        public Schema Schema => AvroData.Schema;
        public GenericRecord AvroData { get; private set; }

        public bool Equals(GenericError other)
        {
            return
                Message == other.Message &&
                AvroData.Equals(other.AvroData);
        }
    }
}
