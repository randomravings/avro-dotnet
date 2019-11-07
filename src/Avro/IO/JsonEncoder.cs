using Avro.Schema;
using Avro.Types;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Avro.IO
{
    public class JsonEncoder : IAvroEncoder
    {
        private delegate int EncodeDelegate(JsonTextWriter writer, int index, Action action);
        private readonly bool _leaveOpen = false;
        private readonly TextWriter _stream;
        private readonly JsonTextWriter _writer;

        private readonly List<EncodeDelegate> _actions;
        private int _index = 0;
        private readonly Stack<int> _loops = new Stack<int>();
        private readonly Stack<int> _skips = new Stack<int>();

        public JsonEncoder(TextWriter stream, AvroSchema schema, string delimiter, bool leaveOpen)
        {
            _stream = stream;
            _writer = new JsonTextWriter(_stream);
            _leaveOpen = leaveOpen;

            _actions = new List<EncodeDelegate>();
            var writer = Expression.Parameter(typeof(JsonTextWriter));
            var index = Expression.Parameter(typeof(int));
            var action = Expression.Parameter(typeof(Action));
            var actionExpressions = new List<Expression<EncodeDelegate>>();

            var pre = new List<Expression>();
            var post = new List<Expression>();

            if (!(schema is RecordSchema))
            {
                pre.Add(Expression.Call(writer, typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteStartObject))));
                post.Add(Expression.Call(writer, typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteEndObject))));
            }

            AppendAction(actionExpressions, schema, pre, post, writer, index, action);
            _actions = actionExpressions.Select(r => r.Compile()).ToList();
        }

        private static void AppendAction(List<Expression<EncodeDelegate>> actions, AvroSchema schema, List<Expression> pre, List<Expression> post, ParameterExpression writer, ParameterExpression index, ParameterExpression action)
        {
            var startIndex = actions.Count;
            switch (schema)
            {
                case RecordSchema r:
                    for (var i = 0; i < r.Count; i++)
                    {
                        var localPre = new List<Expression>();
                        var localPost = new List<Expression>();
                        if (i == 0)
                        {
                            localPre = pre;
                            localPre.Add(Expression.Call(writer, typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteStartObject))));
                        }
                        if (i == r.Count - 1)
                        {
                            localPost = post;
                            localPost.Add(Expression.Call(writer, typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteEndObject))));
                        }
                        localPre.Add(Expression.Call(writer, typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WritePropertyName), new[] { typeof(string) }), Expression.Constant(r[i].Name)));
                        AppendAction(actions, r[i].Type, localPre, localPost, writer, index, action);
                    }
                    break;
                case ArraySchema r:
                    actions.Add(
                        Expression.Lambda<EncodeDelegate>(
                            Expression.Constant(0),
                            writer,
                            index,
                            action
                        )
                    );

                    AppendAction(actions, r.Items, new List<Expression>(), new List<Expression>(), writer, index, action);

                    pre.Add(Expression.Invoke(action));
                    pre.Add(Expression.Constant(actions.Count));

                    post.Insert(0, Expression.Invoke(action));
                    post.Add(Expression.Constant(1));

                    actions[startIndex] =
                        Expression.Lambda<EncodeDelegate>(
                            Expression.Block(pre),
                            writer,
                            index,
                            action
                        );

                    actions.Add(
                        Expression.Lambda<EncodeDelegate>(
                            Expression.Block(post),
                            writer,
                            index,
                            action
                        )
                    );
                    break;
                case MapSchema r:
                    actions.Add(
                        Expression.Lambda<EncodeDelegate>(
                            Expression.Constant(0),
                            writer,
                            index,
                            action
                        )
                    );

                    AppendAction(actions, r.Values, new List<Expression>(), new List<Expression>(), writer, index, action);

                    pre.Add(Expression.Invoke(action));
                    pre.Add(Expression.Constant(actions.Count));

                    post.Insert(0, Expression.Invoke(action));
                    post.Add(Expression.Constant(1));

                    actions[startIndex] =
                        Expression.Lambda<EncodeDelegate>(
                            Expression.Block(pre),
                            writer,
                            index,
                            action
                        );

                    actions.Add(
                        Expression.Lambda<EncodeDelegate>(
                            Expression.Block(post),
                            writer,
                            index,
                            action
                        )
                    );
                    break;
                case UnionSchema r:
                    actions.Add(
                        Expression.Lambda<EncodeDelegate>(
                            Expression.Constant(0),
                            writer,
                            index,
                            action
                        )
                    );

                    actions.Add(
                        Expression.Lambda<EncodeDelegate>(
                            Expression.Constant(0),
                            writer,
                            index,
                            action
                        )
                    );

                    var indexes = new int[r.Count];
                    for (int i = 0; i < r.Count; i++)
                    {
                        indexes[i] = actions.Count();
                        var unionPre = new List<Expression>(pre);
                        var unionPost = new List<Expression>(post);

                        if (!typeof(NullSchema).Equals(r[i].GetType()))
                        {
                            unionPre.Add(Expression.Call(writer, typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteStartObject))));
                            unionPre.Add(Expression.Call(writer, typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WritePropertyName), new[] { typeof(string) }), Expression.Constant(r[i].ToString())));
                            unionPost.Insert(0, Expression.Call(writer, typeof(JsonTextWriter).GetMethod(nameof(JsonTextWriter.WriteEndObject))));
                        }

                        AppendAction(actions, r[i], unionPre, unionPost, writer, index, action);
                    }

                    actions[startIndex] =
                        Expression.Lambda<EncodeDelegate>(
                            Expression.Constant(actions.Count()),
                            writer,
                            index,
                            action
                        );

                    actions[startIndex + 1] =
                        Expression.Lambda<EncodeDelegate>(
                            Expression.MakeIndex(
                                Expression.Constant(indexes),
                                typeof(int[]).GetProperty("Item", new[] { typeof(int) }),
                                new[] { index }
                            ),
                            writer,
                            index,
                            action
                        );

                    break;
                default:
                    var ops = new List<Expression>();
                    ops.AddRange(pre);
                    ops.Add(Expression.Invoke(action));
                    ops.AddRange(post);
                    ops.Add(Expression.Constant(1));
                    actions.Add(
                        Expression.Lambda<EncodeDelegate>(
                            Expression.Block(ops),
                            writer,
                            index,
                            action
                        )
                    );
                    break;
            }
        }

        private int Increment()
        {
            var i = _index;
            _index = (_index + 1) % _actions.Count;
            return i;
        }

        public JsonEncoder(TextWriter stream, AvroSchema schema, string delimiter)
            : this(stream, schema, delimiter, false) { }

        public JsonEncoder(TextWriter stream, AvroSchema schema, bool leaveOpen)
            : this(stream, schema, string.Empty, leaveOpen) { }

        public JsonEncoder(TextWriter stream, AvroSchema schema)
        : this(stream, schema, string.Empty, false) { }

        public void WriteArray<T>(IList<T> items, Action<IAvroEncoder, T> itemsWriter)
        {
            var skip = _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteStartArray());
            _skips.Push(skip);
            _loops.Push(_index);
            foreach (var item in items)
            {
                _index = _loops.Peek();
                itemsWriter.Invoke(this, item);
            }
            _loops.Pop();
            _index = _skips.Pop();
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteEndArray());
        }

        public void WriteArray<A, T>(A items, Action<IAvroEncoder, T> itemsWriter) where A : notnull, IList<T> => WriteArray<T>(items, itemsWriter);

        public void WriteArrayStart()
        {
            var skip = _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteStartArray());
            _skips.Push(skip);
            _loops.Push(_index);
        }

        public void WriteArrayBlock<T>(IList<T> items, Action<IAvroEncoder, T> itemsWriter)
        {
            foreach (var item in items)
            {
                _index = _loops.Peek();
                itemsWriter.Invoke(this, item);
            }
        }

        public void WriteArrayBlock<A, T>(A items, Action<IAvroEncoder, T> itemsWriter) where A : notnull, IList<T> => WriteArrayBlock<T>(items, itemsWriter);

        public void WriteArrayEnd()
        {
            _loops.Pop();
            _index = _skips.Pop();
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteEndArray());
        }

        public void WriteBoolean(bool value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value));
        }

        public void WriteBytes(byte[] value)
        {
            var sb = new StringBuilder();
            foreach (var b in value)
            {
                sb.Append("\\u00");
                sb.Append(b.ToString("X2"));
            }
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(sb.ToString()));
        }

        public void WriteDate(DateTime value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value.ToString("yyyy-MM-dd")));
        }

        public void WriteDecimal(decimal value, int scale)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value));
        }

        public void WriteDecimal(decimal value, int scale, int len)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value));
        }

        public void WriteDouble(double value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value));
        }

        public void WriteDuration(AvroDuration value)
        {
            var bytes = new byte[12];

            bytes[0] = (byte)((value.Months >> 24) & 0xFF);
            bytes[1] = (byte)((value.Months >> 16) & 0xFF);
            bytes[2] = (byte)((value.Months >> 8) & 0xFF);
            bytes[3] = (byte)((value.Months) & 0xFF);

            bytes[4] = (byte)((value.Days >> 24) & 0xFF);
            bytes[5] = (byte)((value.Days >> 16) & 0xFF);
            bytes[6] = (byte)((value.Days >> 8) & 0xFF);
            bytes[7] = (byte)((value.Days) & 0xFF);

            bytes[8] = (byte)((value.MilliSeconds >> 24) & 0xFF);
            bytes[9] = (byte)((value.MilliSeconds >> 16) & 0xFF);
            bytes[10] = (byte)((value.MilliSeconds >> 8) & 0xFF);
            bytes[11] = (byte)((value.MilliSeconds) & 0xFF);

            var sb = new StringBuilder();
            foreach (var b in bytes)
            {
                sb.Append("\\u00");
                sb.Append(b.ToString("x2"));
            }
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(sb.ToString()));
        }

        public void WriteEnum<T>(T value) where T : struct, Enum
        {
            _actions[Increment()].Invoke(_writer, (int)(object)value, () => _writer.WriteValue(value.ToString()));
        }

        public void WriteEnum(IAvroEnum value)
        {
            _actions[Increment()].Invoke(_writer, value.Value, () => _writer.WriteValue(value.Symbol));
        }

        public void WriteFixed<T>(T value) where T : notnull, IAvroFixed => WriteFixed(value.Value);

        public void WriteFixed(byte[] value)
        {
            var sb = new StringBuilder();
            foreach (var b in value)
            {
                sb.Append("\\u00");
                sb.Append(b.ToString("x2"));
            }
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(sb.ToString()));
        }

        public void WriteFloat(float value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value));
        }

        public void WriteInt(int value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value));
        }

        public void WriteLong(long value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value));
        }

        public void WriteMap<T>(IDictionary<string, T> keyValues, Action<IAvroEncoder, T> valuesWriter)
        {
            var skip = _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteStartObject());
            _skips.Push(skip);
            _loops.Push(_index);
            foreach (var keyValue in keyValues)
            {
                _index = _loops.Peek();
                _writer.WritePropertyName(keyValue.Key);
                valuesWriter.Invoke(this, keyValue.Value);
            }
            _loops.Pop();
            _index = _skips.Pop();
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteEndObject());
        }

        public void WriteMap<M, T>(M keyValues, Action<IAvroEncoder, T> valuesWriter) where M : notnull, IDictionary<string, T> => WriteMap<T>(keyValues, valuesWriter);

        public void WriteMapStart()
        {
            var skip = _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteStartObject());
            _skips.Push(skip);
            _loops.Push(_index);
        }

        public void WriteMapBlock<T>(IDictionary<string, T> keyValues, Action<IAvroEncoder, T> valuesWriter)
        {
            foreach (var keyValue in keyValues)
            {
                _index = _loops.Peek();
                _writer.WritePropertyName(keyValue.Key);
                valuesWriter.Invoke(this, keyValue.Value);
            }
        }

        public void WriteMapBlock<M, T>(M keyValues, Action<IAvroEncoder, T> valuesWriter) where M : notnull, IDictionary<string, T> => WriteMapBlock<T>(keyValues, valuesWriter);

        public void WriteMapEnd()
        {
            _loops.Pop();
            _index = _skips.Pop();
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteEndObject());

        }

        public void WriteNull(AvroNull value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteNull());
        }

        public void WriteNullableObject<T>(T? value, Action<IAvroEncoder, T> valueWriter, long nullIndex) where T : class
        {
            var skip = _actions[Increment()].Invoke(_writer, 0, () => { });
            if (value == null)
            {
                _index = _actions[_index].Invoke(_writer, (int)nullIndex, () => { });
                WriteNull(AvroNull.Value);
            }
            else
            {
                _index = _actions[_index].Invoke(_writer, (int)(nullIndex + 1) % 2, () => { });
                valueWriter.Invoke(this, value);
            }
            _index = skip;
        }

        public void WriteNullableValue<T>(T? value, Action<IAvroEncoder, T> valueWriter, long nullIndex) where T : struct
        {
            var skip = _actions[Increment()].Invoke(_writer, 0, () => { });
            if (value.HasValue)
            {
                _index = _actions[_index].Invoke(_writer, (int)(nullIndex + 1) % 2, () => { });
                valueWriter.Invoke(this, value.Value);
            }
            else
            {
                _index = _actions[_index].Invoke(_writer, (int)nullIndex, () => { });
                WriteNull(AvroNull.Value);
            }
            _index = skip;
        }

        public void WriteString(string value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value));
        }

        public void WriteTimeMS(TimeSpan value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value.ToString(@"hh\:mm\:ss\.fff")));
        }

        public void WriteTimeUS(TimeSpan value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value.ToString(@"hh\:mm\:ss\.ffffff")));
        }

        public void WriteTimeNS(TimeSpan value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value.ToString(@"hh\:mm\:ss\.fffffff")));
        }

        public void WriteTimestampMS(DateTime value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value.ToString(@"yyyy-MM-dd HH:mm:ss.fff")));
        }

        public void WriteTimestampUS(DateTime value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value.ToString(@"yyyy-MM-dd HH:mm:ss.ffffff")));
        }

        public void WriteTimestampNS(DateTime value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value.ToString(@"yyyy-MM-dd HH:mm:ss.fffffff")));
        }

        public void WriteUuid(Guid value)
        {
            _actions[Increment()].Invoke(_writer, 0, () => _writer.WriteValue(value));
        }

        public void WriteUnion<T1>(
            AvroUnion<T1> value,
            Action<IAvroEncoder, T1> valueWriter1
        )
            where T1 : notnull
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
            }
        }

        public void WriteUnion<T1, T2>(
            AvroUnion<T1, T2> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2
        )
            where T1 : notnull
            where T2 : notnull
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
                case 1:
                    valueWriter2.Invoke(this, value.GetT2());
                    break;
            }
        }

        public void WriteUnion<T1, T2, T3>(
            AvroUnion<T1, T2, T3> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
                case 1:
                    valueWriter2.Invoke(this, value.GetT2());
                    break;
                case 2:
                    valueWriter3.Invoke(this, value.GetT3());
                    break;
            }
        }

        public void WriteUnion<T1, T2, T3, T4>(
            AvroUnion<T1, T2, T3, T4> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
            where T4 : notnull
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
                case 1:
                    valueWriter2.Invoke(this, value.GetT2());
                    break;
                case 2:
                    valueWriter3.Invoke(this, value.GetT3());
                    break;
                case 3:
                    valueWriter4.Invoke(this, value.GetT4());
                    break;
            }
        }

        public void WriteUnion<T1, T2, T3, T4, T5>(
            AvroUnion<T1, T2, T3, T4, T5> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4,
            Action<IAvroEncoder, T5> valueWriter5
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
            where T4 : notnull
            where T5 : notnull
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
                case 1:
                    valueWriter2.Invoke(this, value.GetT2());
                    break;
                case 2:
                    valueWriter3.Invoke(this, value.GetT3());
                    break;
                case 3:
                    valueWriter4.Invoke(this, value.GetT4());
                    break;
                case 4:
                    valueWriter5.Invoke(this, value.GetT5());
                    break;
            }
        }

        public void Dispose()
        {
            ((IDisposable)_writer).Dispose();
            if (!_leaveOpen && _stream != null)
                _stream.Dispose();
        }
    }
}
