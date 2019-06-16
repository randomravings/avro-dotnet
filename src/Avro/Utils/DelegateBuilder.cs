using Avro.IO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Avro.Utils
{
    public class DelegateBuilder
    {
        public static dynamic BuildReadFunc(Schema schema, Type returnType)
        {
            var streamParameter = Expression.Parameter(typeof(IDecoder), "s");
            var readFunction = typeof(Func<,>).MakeGenericType(typeof(IDecoder), returnType);
            var lambdaExpression =
                Expression.Lambda(
                    readFunction,
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.ReadInt))
                    ),
                    streamParameter
                );
            return lambdaExpression.Compile();
        }

        public static dynamic BuildSkipAction(Schema schema)
        {
            var streamParameter = Expression.Parameter(typeof(IDecoder), "s");
            var skipAction = typeof(Action<>).MakeGenericType(typeof(IDecoder));
            var lambdaExpression =
                Expression.Lambda(
                    skipAction,
                    Expression.Call(
                        streamParameter,
                        typeof(IDecoder).GetMethod(nameof(IDecoder.SkipInt))
                    ),
                    streamParameter
                );
            return lambdaExpression.Compile();
        }

        public static T BuildReadDelegate<T>(MethodInfo method, params object[] missingParamValues)
        {
            var queueMissingParams = new Queue<object>(missingParamValues);

            var dgtMi = typeof(T).GetMethod("Invoke");
            var dgtRet = dgtMi.ReturnType;
            var dgtParams = dgtMi.GetParameters();

            var paramsOfDelegate = dgtParams
                .Select(tp => Expression.Parameter(tp.ParameterType, tp.Name))
                .ToArray();

            var methodParams = method.GetParameters();

            if (method.IsStatic)
            {
                var paramsToPass = methodParams
                    .Select((p, i) => CreateParam(paramsOfDelegate, i, p, queueMissingParams))
                    .ToArray();

                var expr = Expression.Lambda<T>(
                    Expression.Call(method, paramsToPass),
                    paramsOfDelegate);

                return expr.Compile();
            }
            else
            {
                var paramThis = Expression.Convert(paramsOfDelegate[0], method.DeclaringType);

                var paramsToPass = methodParams
                    .Select((p, i) => CreateParam(paramsOfDelegate, i + 1, p, queueMissingParams))
                    .ToArray();

                var expr = Expression.Lambda<T>(
                    Expression.Call(paramThis, method, paramsToPass),
                    paramsOfDelegate);

                return expr.Compile();
            }
        }

        private static Expression CreateParam(ParameterExpression[] paramsOfDelegate, int i, ParameterInfo callParamType, Queue<object> queueMissingParams)
        {
            if (i < paramsOfDelegate.Length)
                return Expression.Convert(paramsOfDelegate[i], callParamType.ParameterType);

            if (queueMissingParams.Count > 0)
                return Expression.Constant(queueMissingParams.Dequeue());

            if (callParamType.ParameterType.IsValueType)
                return Expression.Constant(Activator.CreateInstance(callParamType.ParameterType));

            return Expression.Constant(null);
        }
    }
}
