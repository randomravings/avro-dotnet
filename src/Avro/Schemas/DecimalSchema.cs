using System.Collections.Generic;

namespace Avro.Schemas
{
    public class DecimalSchema : LogicalSchema
    {
        public DecimalSchema()
            : this(new BytesSchema(), 12, 2) { }

        public DecimalSchema(Schema underlyingType)
            : this(new BytesSchema(), 12, 2) { }

        public DecimalSchema(int precision, int scale)
            : this(new BytesSchema(), precision, scale) { }

        public DecimalSchema(Schema underlyingType, int precision, int scale)
            : base(underlyingType, "decimal")
        {
            if (!(underlyingType is BytesSchema || underlyingType is FixedSchema))
                throw new SchemaParseException("Expected 'bytes' or 'fixed' as type for logical type 'decimal'");
            if (precision < 1)
                throw new SchemaParseException("Decimal precision must be a postive non zero number");
            if (scale < 0 || scale > precision)
                throw new SchemaParseException("Decimal scale must be a postive or zero and less or equal to precision");
            Precision = precision;
            Scale = scale;
        }

        public int Precision { get; set; }
        public int Scale { get; set; }
    }
}
