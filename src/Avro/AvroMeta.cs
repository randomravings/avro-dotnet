using Avro.Utils;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Avro
{
    public class AvroMeta
    {
        private readonly TagCollection _tags;

        public AvroMeta()
        {
            _tags = new TagCollection();
        }

        public virtual void AddTags(IEnumerable<KeyValuePair<string, object>> tags)
        {
            foreach (var tag in tags)
                AddTag(tag.Key, tag.Value);
        }

        public virtual void AddTag(string key, object value) => _tags.Add(key, value);
        public virtual void RemoveTag(string key) => _tags.Remove(key);

        public IReadOnlyDictionary<string, object> Tags => _tags;

        private class TagCollection : IReadOnlyDictionary<string, object>
        {
            private readonly IDictionary<string, object> _tags;

            public TagCollection()
            {
                _tags = new Dictionary<string, object>();
            }

            internal void Add(string key, object value)
            {
                if (Constants.RESERVED_TAGS.Contains(key))
                    throw new InvalidOperationException($"'{key}' is a reserved keyword");
                _tags.Add(key, value);
            }

            internal bool Remove(string key) => _tags.Remove(key);

            public object this[string key] => _tags[key];

            public IEnumerable<string> Keys => _tags.Keys;

            public IEnumerable<object> Values => _tags.Values;

            public int Count => _tags.Count;

            public bool ContainsKey(string key) => _tags.ContainsKey(key);

            public IEnumerator<KeyValuePair<string, object>> GetEnumerator() => _tags.GetEnumerator();

            public bool TryGetValue(string key, out object value) => _tags.TryGetValue(key, out value);

            IEnumerator IEnumerable.GetEnumerator() => _tags.GetEnumerator();
        }
    }
}
