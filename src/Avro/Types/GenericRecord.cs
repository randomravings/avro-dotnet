using Avro.Schema;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace Avro.Types
{
    public class GenericRecord : IEquatable<GenericRecord>, IAvroRecord
    {
        private static readonly IReadOnlyDictionary<string, int> EMPTY_INDEX = new ReadOnlyDictionary<string, int>(new Dictionary<string, int>(0));
        private static readonly ValueTuple<int, Func<object>>[] EMPTY_INITIALIZERS = new ValueTuple<int, Func<object>>[0];

        private readonly object[] _values;

        public GenericRecord(RecordSchema schema)
        {
            Schema = schema;
            Index = CreateIndex(schema);
            DefaultInitializers = CreateDefaultInitializers(schema);
            _values = new object[Schema.Count];
            InitializeDefauts();
        }

        public GenericRecord(RecordSchema schema, IReadOnlyDictionary<string, int> index, ValueTuple<int, Func<object>>[] defaultInitializers)
        {
            Schema = schema;
            Index = index ?? EMPTY_INDEX;
            DefaultInitializers = defaultInitializers ?? EMPTY_INITIALIZERS;
            _values = new object[Schema.Count];
            InitializeDefauts();
        }

        public GenericRecord(GenericRecord record, bool copy = false)
        {
            Schema = record.Schema;
            Index = record.Index;
            DefaultInitializers = record.DefaultInitializers;
            if (copy)
            {
                _values = record._values;
            }
            else
            {
                _values = new object[record.Schema.Count];
                InitializeDefauts();
            }
        }

        private void InitializeDefauts()
        {
            foreach (var initializer in DefaultInitializers)
                _values[initializer.Item1] = initializer.Item2.Invoke();
        }

        public bool Equals(GenericRecord other)
        {
            if (!Schema.Equals(other.Schema) || Schema.Count != other.Schema.Count)
                return false;
            if (Index.Count > 0)
            {
                foreach (var key in Index.Keys)
                    if (!CompareValues(this[key], other[key]))
                        return false;
            }
            else
            {
                for (int i = 0; i < Schema.Count; i++)
                    if (!CompareValues(this[i], other[i]))
                        return false;
            }
            return true;
        }

        private bool CompareValues(object a, object b)
        {
            if (a == null ^ b == null)
                return false;
            if (a == null)
                return true;
            return a.Equals(b);
        }

        public RecordSchema Schema { get; private set; }

        public IReadOnlyDictionary<string, int> Index { get; private set; }

        public IReadOnlyList<ValueTuple<int, Func<object>>> DefaultInitializers { get; private set; }

        public int FieldCount => _values.Length;

        public object this[string name]
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

        public object this[int index]
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

        public static IReadOnlyDictionary<string, int> CreateIndex(RecordSchema schema)
        {
            var map = new Dictionary<string, int>(schema.Count);
            var fields = schema.ToArray();
            for (int i = 0; i < fields.Length; i++)
                map.Add(fields[i].Name, i);
            return new ReadOnlyDictionary<string, int>(map);
        }

        public static IReadOnlyList<ValueTuple<int, Func<object>>> CreateDefaultInitializers(RecordSchema schema)
        {
            var initializers = new List<ValueTuple<int, Func<object>>>();
            var fields = schema.ToArray();
            for (int i = 0; i < fields.Length; i++)
                if (fields[i].Default != null)
                    initializers.Add(new ValueTuple<int, Func<object>>(i, GetDefaultInitialization(fields[i].Type, fields[i].Default)));
            return initializers.AsReadOnly();
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
                    sb.Append("#null#");
                }
                else if (value.GetType().Equals(typeof(string)))
                {
                    sb.Append(value);
                }
                else if(typeof(byte[]).IsAssignableFrom(value.GetType()))
                {
                    sb.Append(string.Join(" ", (value as byte[]).Select(r => r.ToString("X2"))));
                }
                else if (typeof(IDictionary).IsAssignableFrom(value.GetType()))
                {
                    int x = 0;
                    var dictEnum = (value as IDictionary).GetEnumerator();
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
                    var arrEnum = (value as IEnumerable).GetEnumerator();
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

        private static Func<object> GetDefaultInitialization(AvroSchema schema, JToken value)
        {
            var defaultInit = default(Func<object>);
            switch (schema)
            {
                case NullSchema _:
                    defaultInit = () => null;
                    break;
                case BooleanSchema _:
                    var boolValue = bool.Parse(value.ToString().ToLower());
                    defaultInit = () => boolValue;
                    break;
                case IntSchema _:
                    var intValue = int.Parse(value.ToString());
                    defaultInit = () => intValue;
                    break;
                case LongSchema _:
                    var longValue = long.Parse(value.ToString());
                    defaultInit = () => long.Parse(value.ToString());
                    break;
                case FloatSchema _:
                    var floatValue = float.Parse(value.ToString());
                    defaultInit = () => floatValue;
                    break;
                case DoubleSchema _:
                    var doubleValue = double.Parse(value.ToString());
                    defaultInit = () => doubleValue;
                    break;
                case BytesSchema _:
                    var byteCodes = value.ToString().Split("\\u", StringSplitOptions.RemoveEmptyEntries);
                    var bytesValue = new byte[byteCodes.Length];
                    for (int i = 0; i < byteCodes.Length; i++)
                        bytesValue[i] = byte.Parse(byteCodes[i], System.Globalization.NumberStyles.HexNumber);
                    defaultInit = () => bytesValue.Clone();
                    break;
                case StringSchema _:
                    var stringValue = value.ToString().Trim('"');
                    defaultInit = () => string.Copy(stringValue);
                    break;
                case ArraySchema a:
                    var arrayItems = value as JArray;
                    var arrayItemsCount = arrayItems.Count;
                    var arrayItemInitializers = new Func<object>[arrayItemsCount];
                    for (int i = 0; i < arrayItemsCount; i++)
                        arrayItemInitializers[i] = GetDefaultInitialization(a.Items, arrayItems[i]);
                    defaultInit = () =>
                    {
                        var array = new List<object>();
                        foreach (var arrayItemInitializer in arrayItemInitializers)
                            array.Add(arrayItemInitializer.Invoke());
                        return array;
                    };
                    break;
                case MapSchema m:
                    var mapItems = (value as JObject).Properties().ToArray();
                    var mapItemsCount = mapItems.Length;
                    var mapItemInitializers = new ValueTuple<string, Func<object>>[mapItemsCount];
                    for (int i = 0; i < mapItemsCount; i++)
                        mapItemInitializers[i] = new ValueTuple<string, Func<object>>(mapItems[i].Name, GetDefaultInitialization(m.Values, mapItems[i].Value));
                    defaultInit = () =>
                    {
                        var map = new Dictionary<string, object>();
                        foreach (var mapItemInitializer in mapItemInitializers)
                            map.Add(mapItemInitializer.Item1, mapItemInitializer.Item2.Invoke());
                        return map;
                    };
                    break;
                case FixedSchema f:
                    var fixedCodes = value.ToString().Split("\\u", StringSplitOptions.RemoveEmptyEntries);
                    var fixedValue = new byte[fixedCodes.Length];
                    for (int i = 0; i < fixedCodes.Length; i++)
                        fixedValue[i] = byte.Parse(fixedCodes[i], System.Globalization.NumberStyles.HexNumber);
                    defaultInit = () => new GenericFixed(f, fixedValue.Clone() as byte[]);
                    break;
                case EnumSchema e:
                    defaultInit = () => new GenericEnum(e, value.ToString().Trim('"'));
                    break;
                case RecordSchema r:
                    var recordFields = r.ToList();
                    var defaultFields =
                        from f in recordFields
                        join p in (value as JObject).Properties() on f.Name equals p.Name
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
                    defaultInit = () =>
                    {
                        var record = new GenericRecord(r);
                        foreach (var fieldInitializer in defaultAssignment)
                            record[fieldInitializer.FieldIndex] = fieldInitializer.Initializer.Invoke();
                        return record;
                    };
                    break;
                case UnionSchema u:
                    defaultInit = GetDefaultInitialization(u[0], value);
                    break;
                case UuidSchema _:
                    defaultInit = () => new Guid(value.ToString().Trim('"'));
                    break;
                case LogicalSchema l:
                    defaultInit = GetDefaultInitialization(l.Type, value);
                    break;
            }
            return defaultInit;
        }
    }
}
