using Avro.Schema;
using Avro.Utils;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace Avro.Types
{
    public abstract class GenericFieldsType<T> where T : FieldsSchema
    {
        private readonly object?[] _values;

        /// <summary>
        /// Creates a new GenericRecord instance.
        /// Name lookups and default expresion resolution will be evaluated.
        /// </summary>
        /// <param name="schema">RecordSchema representing the Avro defintition.</param>
        public GenericFieldsType(T schema)
        {
            Schema = schema;
            Index = CreateIndex(schema);
            DefaultInitializers = CreateDefaultInitializers(schema);
            _values = new object?[Schema.Count];
            InitializeDefaults();
        }

        /// <summary>
        /// Copy Constructor used to copy record structure such as Name index, defaults initialization functions.
        /// This enables faster instantiation by avoiding resolving names to indexes and default expressions.
        /// </summary>
        /// <param name="record">Model record to copy.</param>
        /// <param name="copyData">Indicates if the data should be copied as well. Note this will also disable default initiation.</param>
        public GenericFieldsType(GenericFieldsType<T> record, bool copyData = false)
        {
            Schema = record.Schema;
            Index = record.Index;
            DefaultInitializers = record.DefaultInitializers;
            if (copyData)
            {
                _values = record._values;
            }
            else
            {
                _values = new object[record.Schema.Count];
                InitializeDefaults();
            }
        }

        private void InitializeDefaults()
        {
            foreach (var initializer in DefaultInitializers)
                _values[initializer.Item1] = initializer.Item2.Invoke();
        }

        protected bool CompareValues(object? a, object? b)
        {
            if (a == null && b == null)
                return true;
            if (a == null ^ b == null)
                return false;
            if (typeof(IDictionary).IsAssignableFrom(a.GetType()) && typeof(IDictionary).IsAssignableFrom(b.GetType()))
            {
                var x = ((IDictionary)a).GetEnumerator();
                var y = ((IDictionary)b).GetEnumerator();
                while (x.MoveNext() && y.MoveNext())
                    if (!x.Key.Equals(y.Key) || !x.Value.Equals(y.Value))
                        return false;
                return !(x.MoveNext() ^ y.MoveNext());
            }
            if (typeof(IList).IsAssignableFrom(a.GetType()) && typeof(IList).IsAssignableFrom(b.GetType()))
            {
                var x = ((IList)a).GetEnumerator();
                var y = ((IList)b).GetEnumerator();
                while (x.MoveNext() && y.MoveNext())
                    if (!x.Current.Equals(y.Current))
                        return false;
                return !(x.MoveNext() ^ y.MoveNext());
            }
            if (typeof(IAvroRecord).IsAssignableFrom(a.GetType()) && typeof(IAvroRecord).IsAssignableFrom(b.GetType()))
            {
                var x = (IAvroRecord)a;
                var y = (IAvroRecord)b;
                return x.Equals(y);
            }
            return a.Equals(b);
        }

        public T Schema { get; private set; }

        public IReadOnlyDictionary<string, int> Index { get; private set; }

        public IReadOnlyList<ValueTuple<int, Func<object>>> DefaultInitializers { get; private set; }

        public int FieldCount => _values.Length;

        public object? this[string name]
        {
            get
            {
                return this[Index[name]];
            }
            set
            {
                this[Index[name]] = value;
            }
        }

        public object? this[int index]
        {
            get
            {
                return _values[index];
            }
            set
            {
                _values[index] = value;
            }
        }

        public static IReadOnlyDictionary<string, int> CreateIndex(T schema)
        {
            var map = new Dictionary<string, int>(schema.Count);
            var fields = schema.ToArray();
            for (int i = 0; i < fields.Length; i++)
                map.Add(fields[i].Name, i);
            return new ReadOnlyDictionary<string, int>(map);
        }

        public static IReadOnlyList<ValueTuple<int, Func<object>>> CreateDefaultInitializers(T schema)
        {
            var initializers = new List<ValueTuple<int, Func<object>>>();
            var fields = schema.ToArray();
            for (int i = 0; i < fields.Length; i++)
                if (!fields[i].Default.Equals(JsonUtil.EmptyDefault))
                    initializers.Add(new ValueTuple<int, Func<object>>(i, GetDefaultInitialization(fields[i].Type, fields[i].Default)));
            return initializers.AsReadOnly();
        }

        private static Func<object> GetDefaultInitialization(AvroSchema schema, JToken value)
        {
            switch (schema)
            {
                case NullSchema _:
                    return () => AvroNull.Value;
                case BooleanSchema _:
                    var boolValue = bool.Parse(value.ToString().ToLower());
                    return () => boolValue;
                case IntSchema _:
                    var intValue = int.Parse(value.ToString());
                    return () => intValue;
                case LongSchema _:
                    var longValue = long.Parse(value.ToString());
                    return () => long.Parse(value.ToString());
                case FloatSchema _:
                    var floatValue = float.Parse(value.ToString());
                    return () => floatValue;
                case DoubleSchema _:
                    var doubleValue = double.Parse(value.ToString());
                    return () => doubleValue;
                case BytesSchema _:
                    var byteCodes = value.ToString().Split("\\u", StringSplitOptions.RemoveEmptyEntries);
                    var bytesValue = new byte[byteCodes.Length];
                    for (int i = 0; i < byteCodes.Length; i++)
                        bytesValue[i] = byte.Parse(byteCodes[i], System.Globalization.NumberStyles.HexNumber);
                    return () => bytesValue.Clone();
                case StringSchema _:
                    var stringValue = value.ToString().Trim('"');
                    return () => string.Copy(stringValue);
                case ArraySchema a:
                    var arrayItems = (JArray)value;
                    var arrayItemsCount = arrayItems.Count;
                    var arrayItemInitializers = new Func<object>[arrayItemsCount];
                    for (int i = 0; i < arrayItemsCount; i++)
                        arrayItemInitializers[i] = GetDefaultInitialization(a.Items, arrayItems[i]);
                    return () =>
                    {
                        var array = new List<object>();
                        foreach (var arrayItemInitializer in arrayItemInitializers)
                            array.Add(arrayItemInitializer.Invoke());
                        return array;
                    };
                case MapSchema m:
                    var mapItems = ((JObject)value).Properties().ToArray();
                    var mapItemsCount = mapItems.Length;
                    var mapItemInitializers = new ValueTuple<string, Func<object>>[mapItemsCount];
                    for (int i = 0; i < mapItemsCount; i++)
                        mapItemInitializers[i] = new ValueTuple<string, Func<object>>(mapItems[i].Name, GetDefaultInitialization(m.Values, mapItems[i].Value));
                    return () =>
                    {
                        var map = new Dictionary<string, object>();
                        foreach (var mapItemInitializer in mapItemInitializers)
                            map.Add(mapItemInitializer.Item1, mapItemInitializer.Item2.Invoke());
                        return map;
                    };
                case FixedSchema f:
                    var fixedCodes = value.ToString().Split("\\u", StringSplitOptions.RemoveEmptyEntries);
                    var fixedValue = new byte[fixedCodes.Length];
                    for (int i = 0; i < fixedCodes.Length; i++)
                        fixedValue[i] = byte.Parse(fixedCodes[i], System.Globalization.NumberStyles.HexNumber);
                    return () => new GenericFixed(f, (byte[])fixedValue.Clone());
                case EnumSchema e:
                    return () => new GenericEnum(e, value.ToString().Trim('"'));
                case RecordSchema r:
                    var recordFields = r.ToList();
                    var defaultFields =
                        from f in recordFields
                        join p in ((JObject)value).Properties() on f.Name equals p.Name
                        select new
                        {
                            Field = f,
                            p.Value
                        }
                    ;
                    var defaultAssignment =
                        from d in defaultFields
                        select new
                        {
                            FieldIndex = recordFields.IndexOf(d.Field),
                            Initializer = GetDefaultInitialization(d.Field.Type, d.Value)
                        }
                    ;
                    return () =>
                    {
                        var record = new GenericRecord(r);
                        foreach (var fieldInitializer in defaultAssignment)
                            record[fieldInitializer.FieldIndex] = fieldInitializer.Initializer.Invoke();
                        return record;
                    };
                case UnionSchema u:
                    return GetDefaultInitialization(u[0], value);
                case UuidSchema _:
                    return () => new Guid(value.ToString().Trim('"'));
                case LogicalSchema l:
                    return GetDefaultInitialization(l.Type, value);
                default:
                    return () => AvroNull.Value;
            }
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append('{');
            var i = 0;
            foreach (var value in _values)
            {
                if (i++ > 0)
                    sb.Append("|");
                if (value == null)
                {
                    continue;
                }
                else if (value.GetType().Equals(typeof(string)))
                {
                    sb.Append(value);
                }
                else if (typeof(byte[]).IsAssignableFrom(value.GetType()))
                {
                    sb.Append(string.Join(" ", (value as byte[]).Select(r => r.ToString("X2"))));
                }
                else if (typeof(IDictionary).IsAssignableFrom(value.GetType()))
                {
                    int x = 0;
                    var dictEnum = ((IDictionary)value).GetEnumerator();
                    while (dictEnum.MoveNext())
                    {
                        if (x++ > 0)
                            sb.Append(",");
                        sb.Append($"<{dictEnum.Key?.ToString() ?? "#null#"}:{dictEnum.Value?.ToString() ?? "#null#"}>");
                    }
                }
                else if (typeof(IEnumerable).IsAssignableFrom(value.GetType()))
                {
                    int x = 0;
                    var arrEnum = ((IEnumerable)value).GetEnumerator();
                    sb.Append("[");
                    while (arrEnum.MoveNext())
                    {
                        if (x++ > 0)
                            sb.Append(",");
                        sb.Append(arrEnum.Current?.ToString() ?? "#null#");
                    }
                    sb.Append("]");
                }
                else
                {
                    sb.Append(value.ToString());
                }
            }
            sb.Append('}');
            return sb.ToString();
        }

        public override int GetHashCode() => HashCode.Combine(_values, Schema, FieldCount);
    }
}
