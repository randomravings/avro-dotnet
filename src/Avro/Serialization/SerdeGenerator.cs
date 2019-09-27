using System;
using System.Linq.Expressions;

namespace Avro.Serialization
{
    public static partial class AvroSerializer
    {
        public static Expression CreateReadExpression(AvroSchema schema, Type type)
        {
            var expr = default(Expression);
            var types = SerializationMap.GetTypes(schema);
            return expr;
        }

        public static Expression CreateSkipExpression<T>(AvroSchema schema, Type type)
        {
            var expr = default(Expression);
            var types = SerializationMap.GetTypes(schema);
            return expr;
        }

        public static Expression CreateWriteExpression(AvroSchema schema, Type type)
        {
            var expr = default(Expression);
            var types = SerializationMap.GetTypes(schema);
            return expr;
        }
    }
}
