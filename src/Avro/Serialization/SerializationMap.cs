using Avro.Schema;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Avro.Serialization
{
    public static class SerializationMap
    {
        public static IEnumerable<SerializationType> GetTypes(AvroSchema avroSchema)
        {
            var attributes = avroSchema.GetType().GetCustomAttributes<SerializationType>();
            switch (avroSchema)
            {
                case LogicalSchema l:
                    attributes = attributes.Union(GetTypes(l.Type));
                    break;
                case UnionSchema u:
                    foreach (var s in u)
                        attributes = attributes.Union(GetTypes(s));
                    break;
            }
            return attributes;
        }
    }
}
