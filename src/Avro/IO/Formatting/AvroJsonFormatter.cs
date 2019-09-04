using Avro.Schema;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Avro.IO.Formatting
{
    public class AvroJsonFormatter
    {
        public delegate int Reader(JsonTextReader r, bool q, out string v);
        public delegate int Writer(JsonTextWriter w, bool q, string v);

        public static Reader[] GetReadActions(AvroSchema schema)
        {
            var actions = new List<Expression<Reader>>();

            var readerParameter =
                Expression.Parameter(
                    typeof(JsonTextReader),
                    "w"
                );

            var quoteParameter =
                Expression.Parameter(
                    typeof(bool),
                    "q"
                );

            var valueParameter =
                Expression.Parameter(
                    typeof(string).MakeByRefType(),
                    "v"
                );

            AppendReadAction(
                actions,
                schema,
                null,
                readerParameter,
                quoteParameter,
                valueParameter,
                null,
                null
            );

            return actions.Select(r => r.Compile() as Reader).ToArray();
        }

        public static Writer[] GetWriteActions(AvroSchema schema)
        {
            var actions = new List<Expression<Writer>>();

            var writerParameter =
                Expression.Parameter(
                    typeof(JsonTextWriter),
                    "w"
                );

            var quoteParameter =
                Expression.Parameter(
                    typeof(bool),
                    "q"
                );

            var valueParameter =
                Expression.Parameter(
                    typeof(string),
                    "v"
                );

            AppendWriteAction(
                actions,
                schema,
                null,
                writerParameter,
                quoteParameter,
                valueParameter,
                null,
                null
            );

            return actions.Select(r => r.Compile() as Writer).ToArray();
        }

        private static void AppendReadAction(
            IList<Expression<Reader>> actions,
            AvroSchema schema,
            string key,
            ParameterExpression readerParameter,
            ParameterExpression quoteParameter,
            ParameterExpression valueParameter,
            List<Expression> prepend,
            List<Expression> append
        )
        {
            var expressions = new List<Expression>();
            if (prepend != null)
                expressions = prepend;

            if (key != null)
                expressions.Add(
                    Expression.Call(
                        readerParameter,
                        typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))
                    )
                );

            switch (schema)
            {
                case RecordSchema r:
                    var recordPepend = new List<Expression>(expressions);
                    if (key != null)
                        recordPepend.Add(
                            Expression.Call(
                                readerParameter,
                                typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))
                            )
                        );

                    var recordAppend = new List<Expression>()
                    {
                        // Read record close squiggly.
                        Expression.Call(
                            readerParameter,
                            typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))
                        )
                    };

                    if (append != null)
                        recordAppend.AddRange(append);

                    var fields = r.ToArray();
                    for (int i = 0; i < fields.Length; i++)
                    {
                        var p = (i == 0) ? recordPepend : null;
                        var a = (i == fields.Length - 1) ? recordAppend : null;
                        AppendReadAction(
                            actions,
                            fields[i].Type,
                            fields[i].Name,
                            readerParameter,
                            quoteParameter,
                            valueParameter,
                            p,
                            a
                        );
                    }
                    break;

                case ArraySchema a:
                    expressions.Add(
                        // Read object start.
                        Expression.Call(
                            readerParameter,
                            typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))
                        )
                    );
                    expressions.Add(
                        // Read array start.
                        Expression.Call(
                            readerParameter,
                            typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))
                        )
                    );

                    var arrayStartIndex = actions.Count;
                    actions.Add(null);

                    AppendReadAction(
                        actions,
                        a.Items,
                        null,
                        readerParameter,
                        quoteParameter,
                        valueParameter,
                        null,
                        null
                    );

                    expressions.Add(
                        Expression.Constant(
                            actions.Count,
                            typeof(int)
                        )
                    );

                    actions[arrayStartIndex] =
                        Expression.Lambda<Reader>(
                            Expression.Block(
                                typeof(int),
                                expressions
                            ),
                            readerParameter,
                            quoteParameter,
                            valueParameter
                        );

                    var arrayAppandExpression = new List<Expression>();
                    if (append != null)
                        arrayAppandExpression.AddRange(append);
                    arrayAppandExpression.Add(
                        // Read array end.
                        Expression.Call(
                            readerParameter,
                            typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))
                        )
                    );

                    arrayAppandExpression.Add(
                        Expression.Constant(0)
                    );

                    actions.Add(
                        Expression.Lambda<Reader>(
                            Expression.Block(
                                typeof(int),
                                arrayAppandExpression
                            ),
                            readerParameter,
                            quoteParameter,
                            valueParameter
                        )
                    );
                    break;

                case MapSchema m:
                    expressions.Add(
                        // Read object start.
                        Expression.Call(
                            readerParameter,
                            typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))
                        )
                    );

                    var mapStartIndex = actions.Count;
                    actions.Add(null);

                    var mapLoopVariable =
                        Expression.Variable(
                            typeof(int),
                            "loop"
                        );

                    actions.Add(
                        Expression.Lambda<Reader>(
                            Expression.Block(
                                typeof(int),
                                new[] {
                                    mapLoopVariable
                                },
                                Expression.Assign(
                                    mapLoopVariable,
                                    Expression.Constant(1)
                                ),
                                Expression.Call(
                                    readerParameter,
                                    typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))
                                ),
                                Expression.Assign(
                                    valueParameter,
                                    Expression.TypeAs(
                                        Expression.Property(
                                            readerParameter,
                                            typeof(JsonTextReader).GetProperty(nameof(JsonTextReader.Value))
                                        ),
                                        typeof(string)
                                    )
                                ),
                                Expression.IfThen(
                                    Expression.Equal(
                                        Expression.Property(
                                            readerParameter,
                                            typeof(JsonTextReader).GetProperty(nameof(JsonTextReader.TokenType))
                                        ),
                                        Expression.Constant(
                                            JsonToken.EndObject
                                        )
                                    ),
                                    Expression.Assign(
                                        mapLoopVariable,
                                        Expression.Constant(0)
                                    )
                                ),
                                mapLoopVariable
                            ),
                            readerParameter,
                            quoteParameter,
                            valueParameter
                        )
                    );

                    AppendReadAction(
                        actions,
                        m.Values,
                        null,
                        readerParameter,
                        quoteParameter,
                        valueParameter,
                        null,
                        null
                    );

                    expressions.Add(
                        Expression.Constant(
                            actions.Count,
                            typeof(int)
                        )
                    );

                    actions[mapStartIndex] =
                        Expression.Lambda<Reader>(
                            Expression.Block(
                                typeof(int),
                                expressions
                            ),
                            readerParameter,
                            quoteParameter,
                            valueParameter
                        );

                    var mapAppandExpression = new List<Expression>();
                    if (append != null)
                        mapAppandExpression.AddRange(append);

                    mapAppandExpression.Add(
                        Expression.Constant(0)
                    );

                    actions.Add(
                        Expression.Lambda<Reader>(
                            Expression.Block(
                                typeof(int),
                                mapAppandExpression
                            ),
                            readerParameter,
                            quoteParameter,
                            valueParameter
                        )
                    );
                    break;

                case EnumSchema e:

                    var enumSwitchCases = new List<SwitchCase>();

                    var symbolVariable =
                        Expression.Variable(
                            typeof(string),
                            "symbol"
                        );

                    expressions.Add(
                        Expression.Assign(
                            symbolVariable,
                            Expression.Call(
                                readerParameter,
                                typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.ReadAsString))
                            )
                        )
                    );

                    for (int i = 0; i < e.Symbols.Count; i++)
                    {
                        enumSwitchCases.Add(
                            Expression.SwitchCase(
                                Expression.Assign(
                                    valueParameter,
                                    Expression.Constant(
                                        i.ToString(),
                                        typeof(string)
                                    )
                                ),
                                Expression.Constant(
                                    e.Symbols[i],
                                    typeof(string)
                                )
                            )
                        );
                    }

                    expressions.Add(
                        Expression.Switch(
                            typeof(void),
                            symbolVariable,
                            Expression.Throw(
                                Expression.Constant(
                                    new IndexOutOfRangeException()
                                )
                            ),
                            null,
                            enumSwitchCases.ToArray()
                        )
                    );

                    if (append != null)
                        expressions.AddRange(append);

                    expressions.Add(
                        Expression.Constant(0)
                    );

                    actions.Add(
                        Expression.Lambda<Reader>(
                            Expression.Block(
                                typeof(int),
                                new []
                                {
                                    symbolVariable
                                },
                                expressions
                            ),
                            readerParameter,
                            quoteParameter,
                            valueParameter
                        )
                    );

                    break;

                case UnionSchema u:

                    expressions.Add(
                        // Read object start.
                        Expression.Call(
                            readerParameter,
                            typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))
                        )
                    );
                    expressions.Add(
                        // Read object key if any.
                        Expression.IfThen(
                            Expression.NotEqual(
                                Expression.Property(
                                    readerParameter,
                                    typeof(JsonTextReader).GetProperty(nameof(JsonTextReader.TokenType))
                                ),
                                Expression.Constant(
                                    JsonToken.Null,
                                    typeof(JsonToken)
                                )
                            ),
                            Expression.Call(
                                readerParameter,
                                typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))
                            )
                        )
                    );

                    var unionAppend = new List<Expression>()
                    {
                        // Read end object
                        Expression.Call(
                            readerParameter,
                            typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))
                        )
                    };
                    if (key != null)
                        unionAppend.Add(
                            Expression.IfThen(
                                Expression.NotEqual(
                                    Expression.Property(
                                        readerParameter,
                                        typeof(JsonTextReader).GetProperty(nameof(JsonTextReader.Value))
                                    ),
                                    Expression.Constant(
                                        null,
                                        typeof(object)
                                    )
                                ),
                                Expression.Call(
                                    readerParameter,
                                    typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))
                                )
                            )
                        );

                    var unionSwitchCases = new List<SwitchCase>();

                    for (int i = 0; i < u.Count; i++)
                    {
                        switch (u[i])
                        {
                            case NullSchema _:
                                unionSwitchCases.Add(
                                    Expression.SwitchCase(
                                        Expression.Assign(
                                            valueParameter,
                                            Expression.Constant(
                                                i.ToString(),
                                                typeof(string)
                                            )
                                        ),
                                        Expression.Constant(
                                            null,
                                            typeof(string)
                                        )
                                    )
                                );
                                break;
                            case NamedSchema n:
                                unionSwitchCases.Add(
                                    Expression.SwitchCase(
                                        Expression.Assign(
                                            valueParameter,
                                            Expression.Constant(
                                                i.ToString(),
                                                typeof(string)
                                            )
                                        ),
                                        Expression.Constant(
                                            n.FullName,
                                            typeof(string)
                                        )
                                    )
                                );
                                break;
                            default:
                                unionSwitchCases.Add(
                                    Expression.SwitchCase(
                                        Expression.Assign(
                                            valueParameter,
                                            Expression.Constant(
                                                i.ToString(),
                                                typeof(string)
                                            )
                                        ),
                                        Expression.Constant(
                                            u[i].ToString(),
                                            typeof(string)
                                        )
                                    )
                                );
                                break;
                        }
                    }

                    expressions.Add(
                        Expression.Switch(
                            typeof(void),
                            Expression.TypeAs(
                                Expression.Property(
                                    readerParameter,
                                    typeof(JsonTextReader).GetProperty(nameof(JsonTextReader.Value))
                                ),
                                typeof(string)
                            ),
                            Expression.Throw(
                                Expression.Constant(
                                    new IndexOutOfRangeException()
                                )
                            ),
                            null,
                            unionSwitchCases.ToArray()
                        )
                    );

                    expressions.Add(
                        Expression.Constant(0)
                    );

                    actions.Add(
                        Expression.Lambda<Reader>(
                            Expression.Block(
                                typeof(int),
                                expressions
                            ),
                            readerParameter,
                            quoteParameter,
                            valueParameter
                        )
                    );

                    AppendReadAction(
                        actions,
                        null,
                        null,
                        readerParameter,
                        quoteParameter,
                        valueParameter,
                        null,
                        unionAppend
                    );
                    break;

                default:
                    expressions.Add(
                        Expression.Assign(
                            valueParameter,
                            Expression.Call(
                                readerParameter,
                                typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.ReadAsString))
                            )
                        )
                    );

                    if (append != null)
                        expressions.AddRange(
                            append
                        );

                    expressions.Add(
                        Expression.Constant(
                            0,
                            typeof(int)
                        )
                    );


                    actions.Add(
                        Expression.Lambda<Reader>(
                            Expression.Block(
                                typeof(int),
                                expressions
                            ),
                            readerParameter,
                            quoteParameter,
                            valueParameter
                        )
                    );
                    break;
            }
        }

        private static void AppendWriteAction(
            IList<Expression<Writer>> actions,
            AvroSchema schema,
            string key,
            ParameterExpression writerParameter,
            ParameterExpression quoteParameter,
            ParameterExpression valueParameter,
            List<Expression> prepend,
            List<Expression> append
        )
        {
            var expressions = new List<Expression>();
            if (prepend != null)
                expressions = prepend;

            if (key != null)
                expressions.Add(
                    Expression.Call(
                        writerParameter,
                        typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WritePropertyName), new Type[] { typeof(string) }),
                        Expression.Constant(
                            key,
                            typeof(string)
                        )
                    )
                );

            switch (schema)
            {
                case RecordSchema r:
                    var recordPepend = new List<Expression>(expressions)
                    {
                        Expression.Call(
                            writerParameter,
                            typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteStartObject))
                        )
                    };

                    var recordAppend = new List<Expression>()
                    {
                        Expression.Call(
                            writerParameter,
                            typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteEndObject))
                        )
                    };

                    if (append != null)
                        recordAppend.AddRange(append);

                    var fields = r.ToArray();
                    for (int i = 0; i < fields.Length; i++)
                    {
                        var p = (i == 0) ? recordPepend : null;
                        var a = (i == fields.Length - 1) ? recordAppend : null;
                        AppendWriteAction(
                            actions,
                            fields[i].Type,
                            fields[i].Name,
                            writerParameter,
                            quoteParameter,
                            valueParameter,
                            p,
                            a
                        );
                    }
                    break;
                case ArraySchema a:
                    expressions.Add(
                        Expression.Call(
                            writerParameter,
                            typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteStartArray))
                        )
                    );

                    var arrayStartIndex = actions.Count;
                    actions.Add(null);

                    AppendWriteAction(
                        actions,
                        a.Items,
                        null,
                        writerParameter,
                        quoteParameter,
                        valueParameter,
                        null,
                        null
                    );

                    expressions.Add(
                        Expression.Constant(
                            actions.Count,
                            typeof(int)
                        )
                    );

                    actions[arrayStartIndex] =
                        Expression.Lambda<Writer>(
                            Expression.Block(
                                typeof(int),
                                expressions
                            ),
                            writerParameter,
                            quoteParameter,
                            valueParameter
                        );

                    var arrayAppandExpression = new List<Expression>()
                    {
                        Expression.Call(
                            writerParameter,
                            typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteEndArray))
                        )
                    };

                    if (append != null)
                        arrayAppandExpression.AddRange(append);

                    arrayAppandExpression.Add(
                        Expression.Constant(0)
                    );

                    actions.Add(
                        Expression.Lambda<Writer>(
                            Expression.Block(
                                typeof(int),
                                arrayAppandExpression
                            ),
                            writerParameter,
                            quoteParameter,
                            valueParameter
                        )
                    );

                    break;
                case MapSchema m:
                    expressions.Add(
                        Expression.Call(
                            writerParameter,
                            typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteStartObject))
                        )
                    );

                    var mapStartIndex = actions.Count;
                    actions.Add(null);

                    AppendWriteAction(
                        actions,
                        m.Values,
                        null,
                        writerParameter,
                        quoteParameter,
                        valueParameter,
                        null,
                        null
                    );

                    expressions.Add(
                        Expression.Constant(
                            actions.Count,
                            typeof(int)
                        )
                    );

                    actions[mapStartIndex] =
                        Expression.Lambda<Writer>(
                            Expression.Block(
                                typeof(int),
                                expressions
                            ),
                            writerParameter,
                            quoteParameter,
                            valueParameter
                        );

                    var mapAppandExpression = new List<Expression>()
                    {
                        Expression.Call(
                            writerParameter,
                            typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteEndObject))
                        )
                    };

                    if (append != null)
                        mapAppandExpression.AddRange(append);

                    mapAppandExpression.Add(
                        Expression.Constant(0)
                    );

                    actions.Add(
                        Expression.Lambda<Writer>(
                            Expression.Block(
                                typeof(int),
                                mapAppandExpression
                            ),
                            writerParameter,
                            quoteParameter,
                            valueParameter
                        )
                    );

                    break;

                case EnumSchema e:

                    var enumSwitchCases = new List<SwitchCase>();

                    for (int i = 0; i < e.Symbols.Count; i++)
                    {
                        enumSwitchCases.Add(
                            Expression.SwitchCase(
                                Expression.Call(
                                    writerParameter,
                                    typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteValue), new Type[] { typeof(string) }),
                                    Expression.Constant(
                                       e.Symbols[i],
                                       typeof(string)
                                    )
                                ),
                                Expression.Constant(
                                    i.ToString(),
                                    typeof(string)
                                )
                            )
                        );
                    }

                    expressions.Add(
                        Expression.Switch(
                            valueParameter,
                            Expression.Throw(
                                Expression.Constant(
                                    new IndexOutOfRangeException()
                                )
                            ),
                            enumSwitchCases.ToArray()
                        )
                    );

                    if (append != null)
                        expressions.AddRange(append);

                    expressions.Add(
                        Expression.Constant(0)
                    );

                    actions.Add(
                        Expression.Lambda<Writer>(
                            Expression.Block(
                                typeof(int),
                                expressions
                            ),
                            writerParameter,
                            quoteParameter,
                            valueParameter
                        )
                    );

                    break;

                case UnionSchema u:

                    var unionAppend = default(List<Expression>);
                    if (key != null)
                        unionAppend = new List<Expression>()
                        {
                            Expression.IfThen(
                                Expression.NotEqual(
                                    valueParameter,
                                    Expression.Constant(
                                        "null",
                                        typeof(string)
                                    )
                                ),
                                Expression.Call(
                                    writerParameter,
                                    typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteEndObject))
                                )
                            )
                        };

                    var unionSwitchCases = new List<SwitchCase>();

                    for (int i = 0; i < u.Count; i++)
                    {
                        var unionSwichCaseExpressions = new List<Expression>();
                        switch(u[i])
                        {
                            case NullSchema _:
                                unionSwichCaseExpressions.Add(
                                    Expression.Empty()
                                );
                                break;
                            case NamedSchema n:
                                if (key != null)
                                    unionSwichCaseExpressions.Add(
                                        Expression.Call(
                                            typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteStartObject))
                                        )
                                    );

                                unionSwichCaseExpressions.Add(
                                    Expression.Call(
                                            writerParameter,
                                            typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WritePropertyName), new Type[] { typeof(string) }),
                                            Expression.Constant(
                                                n.FullName,
                                                typeof(string)
                                            )
                                        )
                                );
                                break;
                            default:
                                if (key != null)
                                    unionSwichCaseExpressions.Add(
                                        Expression.Call(
                                            writerParameter,
                                            typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteStartObject))
                                        )
                                    );

                                unionSwichCaseExpressions.Add(
                                    Expression.Call(
                                        writerParameter,
                                        typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WritePropertyName), new Type[] { typeof(string) }),
                                        Expression.Constant(
                                            u[i].ToString(),
                                            typeof(string)
                                        )
                                    )
                                );
                                break;
                        }

                        unionSwitchCases.Add(
                            Expression.SwitchCase(
                                Expression.Block(
                                    unionSwichCaseExpressions
                                ),
                                Expression.Constant(
                                    i.ToString(),
                                    typeof(string)
                                )
                            )
                        );
                    }

                    expressions.Add(
                        Expression.Switch(
                            valueParameter,
                            Expression.Throw(
                                Expression.Constant(
                                    new IndexOutOfRangeException()
                                )
                            ),
                            unionSwitchCases.ToArray()
                        )
                    );

                    expressions.Add(
                        Expression.Constant(0)
                    );

                    actions.Add(
                        Expression.Lambda<Writer>(
                            Expression.Block(
                                typeof(int),
                                expressions
                            ),
                            writerParameter,
                            quoteParameter,
                            valueParameter
                        )
                    );

                    AppendWriteAction(
                        actions,
                        null,
                        null,
                        writerParameter,
                        quoteParameter,
                        valueParameter,
                        null,
                        unionAppend
                    );

                    break;

                default:
                    expressions.Add(
                        Expression.IfThenElse(
                            Expression.IsTrue(
                                quoteParameter
                            ),
                            Expression.Call(
                                writerParameter,
                                typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteValue), new Type[] { typeof(string) }),
                                valueParameter
                            ),
                            Expression.Call(
                                writerParameter,
                                typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteRawValue)),
                                valueParameter
                            )
                        )
                    );

                    if (append != null)
                        expressions.AddRange(append);

                    expressions.Add(
                        Expression.Constant(0)
                    );

                    actions.Add(
                        Expression.Lambda<Writer>(
                            Expression.Block(
                                typeof(int),
                                expressions
                            ),
                            writerParameter,
                            quoteParameter,
                            valueParameter
                        )
                    );

                    break;
            }
        }
    }
}
