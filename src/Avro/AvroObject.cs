using Avro.Utils;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Avro
{
    public abstract class AvroObject
    {
        private readonly IDictionary<string, object> _tags;

        public AvroObject()
        {
            _tags = new SortedList<string, object>();
        }

        public IReadOnlyDictionary<string, object> Tags => new ReadOnlyDictionary<string, object>(_tags);

        public virtual void AddTag(string key, object value)
        {
            if (Constants.RESERVED_TAGS.Contains(key))
                throw new InvalidOperationException($"'{key}' is a reserved keyword");
            _tags.Add(key, value);
        }

        public virtual void AddTags(IEnumerable<KeyValuePair<string, object>> tags)
        {
            foreach (var tag in tags)
                _tags.Add(tag.Key, tag.Value);
        }

        public virtual void SetTag(string key, object value)
        {
            _tags[key] = value;
        }

        public virtual void RemoveTag(string key)
        {
            _tags.Remove(key);
        }
    }
}
