using Avro.Schemas;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Generic
{
    public class GenericRecord
    {
        private readonly object[] _values;
        private readonly IDictionary<string, int> _fieldMap;

        public GenericRecord(RecordSchema schema)
        {
            Schema = schema;
            _values = new object[schema.Count];
            _fieldMap = new SortedList<string, int>(schema.Count);
            for (int i = 0; i < schema.Count; i++)
                _fieldMap.Add(schema.ElementAt(i).Name, i);
        }

        public RecordSchema Schema { get; private set; }

        public object this[string name]
        {
            get
            {
                var index = _fieldMap[name];
                return this[index];
            }
            set
            {
                var index = _fieldMap[name];
                this[index] = value;
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
    }
}
