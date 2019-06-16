using System.Collections.Generic;

namespace Avro.Schemas
{
    public class LogicalSchema : Schema
    {
        public LogicalSchema(Schema type, string logicalType)
        {
            Type = type;
            LogicalType = logicalType;
        }

        public Schema Type { get; set; }
        public string LogicalType { get; protected set; }
        public override string ToString() => LogicalType;
    }
}
