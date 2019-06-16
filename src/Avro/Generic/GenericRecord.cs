using Avro.Schemas;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Generic
{
    public class GenericRecord
    {
        private class GenericField
        {
            public string FieldName { get; set; }
            public Type FieldType { get; set; }
            public object FieldValue { get; set; }
        }
        public RecordSchema Schema { get; private set; }

        private readonly GenericField[] _values;

        private readonly IDictionary<string, GenericField> _fieldMap;

        public GenericRecord(RecordSchema schema)
        {
            Schema = schema;
            _values = schema.Select(r => new GenericField() { FieldName = r.Name, FieldType = typeof(object), FieldValue = null }).ToArray();
            _fieldMap = _values.ToDictionary(r => r.FieldName);
        }

        public object this[string name]
        {
            get
            {
                return _fieldMap[name].FieldValue;
            }
            set
            {
                _fieldMap[name].FieldValue = value;
            }
        }

        public object this[int index]
        {
            get
            {
                return _values[index].FieldValue;
            }
            set
            {
                _values[index].FieldValue = value;
            }
        }
    }
}
