using Avro.Schemas;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Avro.Generic
{
    public class GenericRecord : IEquatable<GenericRecord>
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

        public GenericRecord(GenericRecord record)
        {
            _values = new object[record.Schema.Count];
            Schema = record.Schema;
            Index = record.Index;
            DefaultInitializers = record.DefaultInitializers;
            _values = new object[Schema.Count];
            InitializeDefauts();
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
                    initializers.Add(new ValueTuple<int, Func<object>>(i, GenericResolver.GetDefaultInitialization(fields[i].Type, fields[i].Default)));
            return initializers.AsReadOnly();
        }

        public override string ToString()
        {
            return string.Join("|", _values.Select(r => r?.ToString() ?? "<null>"));
        }
    }
}
