using Avro.Schema;
using Avro.Types;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;

namespace Avro.IO
{
    public class JsonDecoder : IAvroDecoder
    {
        private delegate int DecodeDelegate(JsonTextReader reader, int index, out string value);

        private readonly TextReader _stream;
        private readonly JsonTextReader _reader;

        private List<DecodeDelegate> _actions;
        private int _index = 0;

        public JsonDecoder(TextReader stream, AvroSchema schema)
        {
            _stream = stream;
            _reader = new JsonTextReader(_stream) { SupportMultipleContent = true };

            _actions = new List<DecodeDelegate>();
            var reader = Expression.Parameter(typeof(JsonTextReader));
            var index = Expression.Parameter(typeof(int));
            var key = Expression.Parameter(typeof(string).MakeByRefType());
            var value = Expression.Parameter(typeof(string).MakeByRefType());
            var actionExpressions = new List<Expression<DecodeDelegate>>();

            var pre = new List<Expression>();
            var post = new List<Expression>();

            if (!(schema is RecordSchema))
            {
                pre.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                post.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
            }

            AppendAction(actionExpressions, string.Empty, schema, pre, post, reader, index, value);
            _actions = actionExpressions.Select(r => r.Compile()).ToList();
            _reader.Read();
        }

        private static void AppendAction(List<Expression<DecodeDelegate>> actions, string key, AvroSchema schema, List<Expression> pre, List<Expression> post, ParameterExpression reader, ParameterExpression index, ParameterExpression value)
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
                            localPre.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                        }
                        if (i == r.Count - 1)
                        {
                            localPost = post;
                            localPost.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                        }
                        if (r[i].Type is RecordSchema)
                        {
                            localPre.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                        }
                        AppendAction(actions, r[i].Name, r[i].Type, localPre, localPost, reader, index, value);
                    }
                    break;
                case ArraySchema r:
                    actions.Add(
                        Expression.Lambda<DecodeDelegate>(
                            Expression.Constant(0),
                            reader,
                            index,
                            value
                        )
                    );

                    AppendAction(actions, string.Empty, r.Items, new List<Expression>(), new List<Expression>(), reader, index, value);

                    pre.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                    pre.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                    pre.Add(Expression.Constant(actions.Count));

                    post.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                    post.Add(Expression.Constant(1));

                    actions[startIndex] =
                        Expression.Lambda<DecodeDelegate>(
                            Expression.Block(pre),
                            reader,
                            index,
                            value
                        );

                    actions.Add(
                        Expression.Lambda<DecodeDelegate>(
                            Expression.Block(post),
                            reader,
                            index,
                            value
                        )
                    );
                    break;
                case MapSchema r:
                    actions.Add(
                        Expression.Lambda<DecodeDelegate>(
                            Expression.Constant(0),
                            reader,
                            index,
                            value
                        )
                    );

                    AppendAction(actions, string.Empty, r.Values, new List<Expression>(), new List<Expression>(), reader, index, value);

                    pre.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                    pre.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                    pre.Add(Expression.Constant(actions.Count));

                    post.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                    post.Add(Expression.Constant(1));

                    actions[startIndex] =
                        Expression.Lambda<DecodeDelegate>(
                            Expression.Block(pre),
                            reader,
                            index,
                            value
                        );

                    actions.Add(
                        Expression.Lambda<DecodeDelegate>(
                            Expression.Block(post),
                            reader,
                            index,
                            value
                        )
                    );
                    break;
                case UnionSchema r:
                    actions.Add(
                        Expression.Lambda<DecodeDelegate>(
                            Expression.Constant(0),
                            reader,
                            index,
                            value
                        )
                    );

                    actions.Add(
                        Expression.Lambda<DecodeDelegate>(
                            Expression.Constant(0),
                            reader,
                            index,
                            value
                        )
                    );

                    var indexes = new int[r.Count];
                    for (int i = 0; i < r.Count; i++)
                    {
                        indexes[i] = actions.Count();
                        var unionPre = new List<Expression>(pre);
                        var unionPost = new List<Expression>(post);

                        var typeKey = string.Empty;
                        if (!typeof(NullSchema).Equals(r[i].GetType()))
                        {
                            typeKey = r[i].ToString();
                            unionPre.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                            unionPost.Insert(0, Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                        }

                        AppendAction(actions, typeKey, r[i], unionPre, unionPost, reader, index, value);
                    }

                    actions[startIndex] =
                        Expression.Lambda<DecodeDelegate>(
                            Expression.Block(
                                Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))),
                                Expression.Constant(actions.Count())
                            ),
                            reader,
                            index,
                            value
                        );

                    actions[startIndex + 1] =
                        Expression.Lambda<DecodeDelegate>(
                            Expression.MakeIndex(
                                Expression.Constant(indexes),
                                typeof(int[]).GetProperty("Item", new[] { typeof(int) }),
                                new[] { index }
                            ),
                            reader,
                            index,
                            value
                        );

                    break;

                case NullSchema r:
                    var nullOps = new List<Expression>();
                    if(key != string.Empty)
                        nullOps.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                    nullOps.AddRange(pre);
                    nullOps.Add(
                        Expression.Assign(
                            value,
                            Expression.Default(typeof(string))         
                        )
                    );
                    nullOps.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                    nullOps.AddRange(post);
                    nullOps.Add(Expression.Constant(1));

                    actions.Add(
                        Expression.Lambda<DecodeDelegate>(
                            Expression.Block(nullOps),
                            reader,
                            index,
                            value
                        )
                    );
                    break;
                case AvroSchema r:
                    var ops = new List<Expression>();
                    ops.AddRange(pre);
                    if (key != string.Empty)
                        ops.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                    ops.Add(
                        Expression.Assign(
                            value,
                            Expression.Call(
                                Expression.MakeMemberAccess(reader, typeof(JsonTextReader).GetProperty(nameof(JsonTextReader.Value))),
                                typeof(object).GetMethod(nameof(object.ToString))
                            )
                        )
                    );
                    ops.Add(Expression.Call(reader, typeof(JsonTextReader).GetMethod(nameof(JsonTextReader.Read))));
                    ops.AddRange(post);
                    ops.Add(Expression.Constant(1));

                    actions.Add(
                        Expression.Lambda<DecodeDelegate>(
                            Expression.Block(ops),
                            reader,
                            index,
                            value
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

        public IList<T> ReadArray<T>(Func<IAvroDecoder, T> itemsReader) => ReadArray<List<T>, T>(itemsReader);
        public IList<T> ReadArrayBlock<T>(Func<IAvroDecoder, T> itemsReader) => ReadArrayBlock<List<T>, T>(itemsReader);
        public bool ReadArrayBlock<T>(Func<IAvroDecoder, T> itemsReader, ref IList<T> array) => ReadArrayBlock(itemsReader, ref array);

        public A ReadArray<A, T>(Func<IAvroDecoder, T> itemsReader) where A : notnull, IList<T>, new()
        {
            var array = new A();
            var end = _actions[Increment()].Invoke(_reader, 0, out _);
            var loop = _index;
            while (_reader.TokenType != JsonToken.EndArray)
            {
                _index = loop;
                array.Add(itemsReader.Invoke(this));
            }
            _index = end;
            _actions[Increment()].Invoke(_reader, 0, out _);
            return array;
        }

        public A ReadArrayBlock<A, T>(Func<IAvroDecoder, T> itemsReader) where A : notnull, IList<T>, new()
        {
            var array = new A();
            ReadArrayBlock(itemsReader, ref array);
            return array;
        }

        public bool ReadArrayBlock<A, T>(Func<IAvroDecoder, T> itemsReader, ref A array) where A : notnull, IList<T>
        {
            var end = _actions[Increment()].Invoke(_reader, 0, out _);
            var loop = _index;
            while (_reader.TokenType != JsonToken.EndArray)
            {
                _index = loop;
                array.Add(itemsReader.Invoke(this));
            }
            _index = end;
            _actions[Increment()].Invoke(_reader, 0, out _);
            return false;
        }

        public bool ReadBoolean()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return bool.Parse(s);
        }

        public byte[] ReadBytes()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return s.Split(new string[] { "\\u00" }, StringSplitOptions.RemoveEmptyEntries).Select(r => Convert.ToByte(r, 16)).ToArray();
        }

        public DateTime ReadDate()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return DateTime.Parse(s);
        }

        public decimal ReadDecimal(int scale)
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return decimal.Parse(s);
        }

        public decimal ReadDecimal(int scale, int len)
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return decimal.Parse(s);
        }

        public double ReadDouble()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return double.Parse(s);
        }

        public AvroDuration ReadDuration()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            var bytes = s.Split(new string[] { "\\u00" }, StringSplitOptions.RemoveEmptyEntries).Select(r => Convert.ToByte(r)).ToArray();

            var mm =
                (uint)(bytes[0] & 0xFF) << 24 |
                (uint)(bytes[1] & 0xFF) << 16 |
                (uint)(bytes[2] & 0xFF) << 8 |
                (uint)(bytes[3] & 0xFF)
            ;

            var dd =
                (uint)(bytes[4] & 0xFF) << 24 |
                (uint)(bytes[5] & 0xFF) << 16 |
                (uint)(bytes[6] & 0xFF) << 8 |
                (uint)(bytes[7] & 0xFF)
            ;

            var ms =
                (uint)(bytes[8] & 0xFF) << 24 |
                (uint)(bytes[9] & 0xFF) << 16 |
                (uint)(bytes[10] & 0xFF) << 8 |
                (uint)(bytes[11] & 0xFF)
            ;

            return new AvroDuration(mm, dd, ms);
        }

        public T ReadEnum<T>() where T : struct, Enum
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return Enum.Parse<T>(s);
        }

        public T ReadEnum<T>(T value) where T : notnull, IAvroEnum
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            value.Symbol = s;
            return value;
        }

        public T ReadFixed<T>(T bytes) where T : notnull, IAvroFixed
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            s.Split(new string[] { "\\u00" }, StringSplitOptions.RemoveEmptyEntries).Select((r, i) => bytes[i] = Convert.ToByte(r));
            return bytes;
        }

        public byte[] ReadFixed(byte[] bytes)
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            s.Split(new string[] { "\\u00" }, StringSplitOptions.RemoveEmptyEntries).Select((r, i) => bytes[i] = Convert.ToByte(r));
            return bytes;
        }

        public byte[] ReadFixed(int len)
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return s.Split(new string[] { "\\u00" }, StringSplitOptions.RemoveEmptyEntries).Select(r => Convert.ToByte(r)).ToArray();
        }

        public float ReadFloat()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return float.Parse(s);
        }

        public int ReadInt()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return int.Parse(s);
        }

        public long ReadLong()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return long.Parse(s);
        }

        public IDictionary<string, T> ReadMap<T>(Func<IAvroDecoder, T> valuesReader) => ReadMap<Dictionary<string, T>, T>(valuesReader);
        public IDictionary<string, T> ReadMapBlock<T>(Func<IAvroDecoder, T> valuesReader) => ReadMapBlock<Dictionary<string, T>, T>(valuesReader);
        public bool ReadMapBlock<T>(Func<IAvroDecoder, T> valuesReader, ref IDictionary<string, T> map) => ReadMapBlock(valuesReader, ref map);

        public M ReadMap<M, T>(Func<IAvroDecoder, T> valuesReader) where M : notnull, IDictionary<string, T>, new()
        {
            var map = new M();
            var end = _actions[Increment()].Invoke(_reader, 0, out _);
            var loop = _index;
            while (_reader.TokenType != JsonToken.EndObject)
            {
                _index = loop;
                var key = _reader.Value.ToString();
                _reader.Read();
                var value = valuesReader.Invoke(this);
                map.Add(key, value);
            }
            _index = end;
            _actions[Increment()].Invoke(_reader, 0, out _);
            return map;
        }

        public M ReadMapBlock<M, T>(Func<IAvroDecoder, T> valuesReader) where M : notnull, IDictionary<string, T>, new()
        {
            var array = new M();
            ReadMapBlock(valuesReader, ref array);
            return array;
        }

        public bool ReadMapBlock<M, T>(Func<IAvroDecoder, T> valuesReader, ref M map) where M : notnull, IDictionary<string, T>
        {

            var end = _actions[Increment()].Invoke(_reader, 0, out _);
            var loop = _index;
            while (_reader.TokenType != JsonToken.EndObject)
            {
                _index = loop;
                var key = _reader.Value.ToString();
                _reader.Read();
                var value = valuesReader.Invoke(this);
                map.Add(key, value);
            }
            _index = end;
            _actions[Increment()].Invoke(_reader, 0, out _);
            return false;
        }

        public AvroNull ReadNull()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return AvroNull.Value;
        }

        public T? ReadNullableObject<T>(Func<IAvroDecoder, T> reader, long nullIndex) where T : class
        {
            var value = default(T?);
            var skip = _actions[Increment()].Invoke(_reader, 0, out _);
            if (_reader.TokenType == JsonToken.Null)
            {
                _index = _actions[_index].Invoke(_reader, (int)nullIndex, out var s);
                ReadNull();
            }
            else
            {
                _index = _actions[_index].Invoke(_reader, (int)(nullIndex + 1) % 2, out var s);
                value = reader.Invoke(this);
            }
            _index = skip;
            return value;
        }

        public T? ReadNullableValue<T>(Func<IAvroDecoder, T> reader, long nullIndex) where T : struct
        {
            var value = default(T?);
            var skip = _actions[Increment()].Invoke(_reader, 0, out _);
            if (_reader.TokenType == JsonToken.Null)
            {
                _index = _actions[_index].Invoke(_reader, (int)nullIndex, out var s);
                ReadNull();
            }
            else
            {
                _index = _actions[_index].Invoke(_reader, (int)(nullIndex + 1) % 2, out var s);
                value = reader.Invoke(this);
            }
            _index = skip;
            return value;
        }

        public string ReadString()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return s;
        }

        public TimeSpan ReadTimeMS()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return TimeSpan.Parse(s);
        }

        public TimeSpan ReadTimeNS()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return TimeSpan.Parse(s);
        }

        public TimeSpan ReadTimeUS()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return TimeSpan.Parse(s);
        }

        public DateTime ReadTimestampMS()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return DateTime.Parse(s);
        }

        public DateTime ReadTimestampNS()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return DateTime.Parse(s);
        }

        public DateTime ReadTimestampUS()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return DateTime.Parse(s);
        }

        public Guid ReadUuid()
        {
            _actions[Increment()].Invoke(_reader, 0, out var s);
            return Guid.Parse(s);
        }

        public AvroUnion<T1> ReadUnion<T1>(
            Func<IAvroDecoder, T1> reader1
        )
            where T1 : notnull
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1>(reader1.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 1);
                    throw ex;
            }
        }

        public AvroUnion<T1, T2> ReadUnion<T1, T2>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2
        )
            where T1 : notnull
            where T2 : notnull
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1, T2>(reader1.Invoke(this));
                case 1:
                    return new AvroUnion<T1, T2>(reader2.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 1);
                    throw ex;
            }
        }

        public AvroUnion<T1, T2, T3> ReadUnion<T1, T2, T3>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2,
            Func<IAvroDecoder, T3> reader3
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1, T2, T3>(reader1.Invoke(this));
                case 1:
                    return new AvroUnion<T1, T2, T3>(reader2.Invoke(this));
                case 2:
                    return new AvroUnion<T1, T2, T3>(reader3.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 2);
                    throw ex;
            }
        }

        public AvroUnion<T1, T2, T3, T4> ReadUnion<T1, T2, T3, T4>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2,
            Func<IAvroDecoder, T3> reader3,
            Func<IAvroDecoder, T4> reader4
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
            where T4 : notnull
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1, T2, T3, T4>(reader1.Invoke(this));
                case 1:
                    return new AvroUnion<T1, T2, T3, T4>(reader2.Invoke(this));
                case 2:
                    return new AvroUnion<T1, T2, T3, T4>(reader3.Invoke(this));
                case 3:
                    return new AvroUnion<T1, T2, T3, T4>(reader4.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 3);
                    throw ex;
            }
        }

        public AvroUnion<T1, T2, T3, T4, T5> ReadUnion<T1, T2, T3, T4, T5>(
            Func<IAvroDecoder, T1> reader1,
            Func<IAvroDecoder, T2> reader2,
            Func<IAvroDecoder, T3> reader3,
            Func<IAvroDecoder, T4> reader4,
            Func<IAvroDecoder, T5> reader5
        )
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
            where T4 : notnull
            where T5 : notnull
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    return new AvroUnion<T1, T2, T3, T4, T5>(reader1.Invoke(this));
                case 1:
                    return new AvroUnion<T1, T2, T3, T4, T5>(reader2.Invoke(this));
                case 2:
                    return new AvroUnion<T1, T2, T3, T4, T5>(reader3.Invoke(this));
                case 3:
                    return new AvroUnion<T1, T2, T3, T4, T5>(reader4.Invoke(this));
                case 4:
                    return new AvroUnion<T1, T2, T3, T4, T5>(reader4.Invoke(this));
                default:
                    var ex = UnionIndexException(index, 4);
                    throw ex;
            }
        }

        public void SkipArray(Action<IAvroDecoder> itemsSkipper)
        {
            throw new NotImplementedException();
        }

        public void SkipBoolean()
        {
            throw new NotImplementedException();
        }

        public void SkipBytes()
        {
            throw new NotImplementedException();
        }

        public void SkipDate()
        {
            throw new NotImplementedException();
        }

        public void SkipDecimal()
        {
            throw new NotImplementedException();
        }

        public void SkipDecimal(int len)
        {
            throw new NotImplementedException();
        }

        public void SkipDouble()
        {
            throw new NotImplementedException();
        }

        public void SkipDuration()
        {
            throw new NotImplementedException();
        }

        public void SkipEnum()
        {
            throw new NotImplementedException();
        }

        public void SkipFixed(int len)
        {
            throw new NotImplementedException();
        }

        public void SkipFloat()
        {
            throw new NotImplementedException();
        }

        public void SkipInt()
        {
            throw new NotImplementedException();
        }

        public void SkipLong()
        {
            throw new NotImplementedException();
        }

        public void SkipMap(Action<IAvroDecoder> valuesSkipper)
        {
            throw new NotImplementedException();
        }

        public void SkipNull()
        {
            throw new NotImplementedException();
        }

        public void SkipNullable(Action<IAvroDecoder> skipper, long nullIndex)
        {
            throw new NotImplementedException();
        }

        public void SkipString()
        {
            throw new NotImplementedException();
        }

        public void SkipTimeMS()
        {
            throw new NotImplementedException();
        }

        public void SkipTimeNS()
        {
            throw new NotImplementedException();
        }

        public void SkipTimestampMS()
        {
            throw new NotImplementedException();
        }

        public void SkipTimestampNS()
        {
            throw new NotImplementedException();
        }

        public void SkipTimestampUS()
        {
            throw new NotImplementedException();
        }

        public void SkipTimeUS()
        {
            throw new NotImplementedException();
        }

        public void SkipUuid()
        {
            throw new NotImplementedException();
        }

        public void SkipUnion<T1>(
            Action<IAvroDecoder> skipper1
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    skipper1.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 1);
                    throw ex;
            }
        }

        public void SkipUnion<T1, T2>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    skipper1.Invoke(this);
                    break;
                case 1:
                    skipper2.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 1);
                    throw ex;
            }
        }

        public void SkipUnion<T1, T2, T3>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    skipper1.Invoke(this);
                    break;
                case 1:
                    skipper2.Invoke(this);
                    break;
                case 2:
                    skipper3.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 2);
                    throw ex;
            }
        }

        public void SkipUnion<T1, T2, T3, T4>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    skipper1.Invoke(this);
                    break;
                case 1:
                    skipper2.Invoke(this);
                    break;
                case 2:
                    skipper3.Invoke(this);
                    break;
                case 3:
                    skipper4.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 3);
                    throw ex;
            }
        }

        public void SkipUnion<T1, T2, T3, T4, T5>(
            Action<IAvroDecoder> skipper1,
            Action<IAvroDecoder> skipper2,
            Action<IAvroDecoder> skipper3,
            Action<IAvroDecoder> skipper4,
            Action<IAvroDecoder> skipper5
        )
        {
            var index = ReadLong();
            switch (index)
            {
                case 0:
                    skipper1.Invoke(this);
                    break;
                case 1:
                    skipper2.Invoke(this);
                    break;
                case 2:
                    skipper3.Invoke(this);
                    break;
                case 3:
                    skipper4.Invoke(this);
                    break;
                case 4:
                    skipper5.Invoke(this);
                    break;
                default:
                    var ex = UnionIndexException(index, 4);
                    throw ex;
            }
        }

        private static IndexOutOfRangeException UnionIndexException(long index, long range)
        {
            return new IndexOutOfRangeException($"Union Index out of range: '{index}'. Valid range [{0}:{range}]");
        }

        public void Dispose() { }
    }
}
