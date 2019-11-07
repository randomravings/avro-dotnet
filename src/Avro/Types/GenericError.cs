using Avro.Schema;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Types
{
    public sealed class GenericError : GenericFieldsType<ErrorSchema>, IEquatable<IAvroError>, IAvroError
    {
        public static GenericError Empty { get; } = new GenericError(AvroParser.ReadSchema<ErrorSchema>(@"{""name"":""com.acme.void.error"",""type"":""error"",""fields"":[]}"));
        public GenericError(ErrorSchema schema)
            : base(schema) { }
        public GenericError(GenericError record, bool copyData = false)
            : base(record, copyData) { }
        public static bool operator ==(GenericError left, GenericError right) =>
            EqualityComparer<GenericError>.Default.Equals(left, right);
        public static bool operator !=(GenericError left, GenericError right) => !(left == right);
        public override bool Equals(object obj) => obj != null && obj is IAvroError && Equals((IAvroError)obj);
        public bool Equals(IAvroError other)
        {
            if (!Schema.Equals(other.Schema) || !Schema.Select(r => r.Name).SequenceEqual(other.Schema.Select(r => r.Name)))
                return false;
            for (int i = 0; i < Schema.Count; i++)
                if (!CompareValues(this[i], other[i]))
                    return false;
            return true;
        }
        public override string ToString() => base.ToString();
        public override int GetHashCode() => base.GetHashCode();
    }
}
