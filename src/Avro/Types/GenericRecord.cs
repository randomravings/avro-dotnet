using Avro.Schema;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Types
{
    public sealed class GenericRecord : GenericFieldsType<RecordSchema>, IEquatable<IAvroRecord>, IAvroRecord
    {
        public static GenericRecord Empty { get; } = new GenericRecord(AvroParser.ReadSchema<RecordSchema>(@"{""name"":""com.acme.void.record"",""type"":""record"",""fields"":[]}"));
        public GenericRecord(RecordSchema schema)
            : base(schema) { }
        public GenericRecord(GenericRecord record, bool copyData = false)
            : base(record, copyData) { }
        public static bool operator ==(GenericRecord left, GenericRecord right) =>
            EqualityComparer<GenericRecord>.Default.Equals(left, right);
        public static bool operator !=(GenericRecord left, GenericRecord right) => !(left == right);
        public override bool Equals(object obj) => obj != null && obj is IAvroRecord && Equals((IAvroRecord)obj);
        public bool Equals(IAvroRecord other)
        {
            if (!Schema.Equals(other.Schema) || !Schema.Select(r => r.Name).SequenceEqual(other.Schema.Select(r => r.Name)))
                return false;
            for (int i = 0; i < Schema.Count; i++)
                if (!CompareValues(this[i], other[i]))
                    return false;
            return true;
        }
        public override string ToString() => base.ToString();
        public override int GetHashCode() => HashCode.Combine(base.GetHashCode());
    }
}
