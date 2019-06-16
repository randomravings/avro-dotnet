using System.Collections.Generic;

namespace Avro.Schemas
{
    public class DateSchema : LogicalSchema
    {
        public DateSchema()
            : base(new IntSchema(), "date") { }
    }
}
