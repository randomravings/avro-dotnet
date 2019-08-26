using Avro.IO.Formatting;
using Avro.Types;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;

namespace Avro.IO
{
    public class JsonDecoder : IDecoder
    {
        private readonly TextReader _stream;
        private readonly AvroSchema _schema;
        private JsonTextReader _reader;
        private readonly IList<JsonItemDescriptor> _headings;

        private int _index = 0;

        public JsonDecoder(TextReader stream, AvroSchema schema, string delimiter = null)
        {
            _stream = stream;
            _schema = schema;
            _reader = new JsonTextReader(_stream);
            _headings = new List<JsonItemDescriptor>();
            JsonItemDescriptor.AddHeading(_schema, _headings);

            _reader.SupportMultipleContent = true;
        }

        private void ReadStart()
        {
            if (_index >= _headings.Count)
            {
                while (_reader.Read() && _reader.Depth > 0) ;
                _index = 0;
            }

            _reader.Read();
            switch (_reader.TokenType)
            {
                case JsonToken.None:
                case JsonToken.StartObject:
                case JsonToken.StartArray:
                    ReadStart();
                    break;

                case JsonToken.PropertyName:
                    var item = _headings[_index++];
                    if (item.PropertyName != _reader.Value.ToString())
                        throw new IOException($"Expected property: '{item.PropertyName}', was: '{_reader.Value}'");
                    _reader.Read();
                    if (item.IsRecord)
                        ReadStart();
                    break;
            }
        }

        public IList<T> ReadArray<T>(Func<IDecoder, T> itemsReader)
        {
            ReadStart();
            var loop = _index;
            var items = new List<T>();
            // Seek First Object or End of Array.
            while (_reader.Read() && (_reader.TokenType == JsonToken.StartObject && _reader.TokenType == JsonToken.EndArray));
            while (_reader.TokenType != JsonToken.EndArray)
            {
                _index = loop;
                items.Add(itemsReader.Invoke(this));
                _reader.Read(); // End Object;
                _reader.Read(); // Start Object or End Array;
            }
            _index = _headings[loop].Limit + 1;
            return items;
        }

        public bool ReadArrayBlock<T>(Func<IDecoder, T> itemsReader, out IList<T> array)
        {
            throw new NotImplementedException();
        }

        public bool ReadBoolean()
        {
            ReadStart();
            return bool.Parse(_reader.Value.ToString());
        }

        public byte[] ReadBytes()
        {
            throw new NotImplementedException();
        }

        public byte[] ReadBytes(byte[] bytes)
        {
            throw new NotImplementedException();
        }

        public DateTime ReadDate()
        {
            throw new NotImplementedException();
        }

        public decimal ReadDecimal(int scale)
        {
            throw new NotImplementedException();
        }

        public decimal ReadDecimal(int scale, int len)
        {
            throw new NotImplementedException();
        }

        public double ReadDouble()
        {
            throw new NotImplementedException();
        }

        public AvroDuration ReadDuration()
        {
            throw new NotImplementedException();
        }

        public byte[] ReadFixed(byte[] bytes)
        {
            throw new NotImplementedException();
        }

        public byte[] ReadFixed(int len)
        {
            throw new NotImplementedException();
        }

        public float ReadFloat()
        {
            throw new NotImplementedException();
        }

        public int ReadInt()
        {
            ReadStart();
            return Convert.ToInt32(_reader.Value);
        }

        public long ReadLong()
        {
            throw new NotImplementedException();
        }

        public IDictionary<string, T> ReadMap<T>(Func<IDecoder, T> valuesReader)
        {
            ReadStart();
            var loop = _index;
            var items = new Dictionary<string, T>();
            // Seek First Object or End of Object.
            while (_reader.Read() && (_reader.TokenType == JsonToken.StartObject && _reader.TokenType == JsonToken.EndObject));
            while (_reader.TokenType != JsonToken.EndObject)
            {
                _index = loop;
                var key = _reader.Value.ToString();
                var value = valuesReader.Invoke(this);
                items.Add(key, value);
                _reader.Read(); // End Object for Value
                _reader.Read(); // PropertyName or End Object for Map
            }
            _index = _headings[loop].Limit + 1;
            return items;
        }

        public bool ReadMapBlock<T>(Func<IDecoder, T> valuesReader, out IDictionary<string, T> map)
        {
            throw new NotImplementedException();
        }

        public AvroNull ReadNull()
        {
            ReadStart();
            return new AvroNull();
        }

        public T ReadNullableObject<T>(Func<IDecoder, T> reader, long nullIndex) where T : class
        {
            throw new NotImplementedException();
        }

        public T? ReadNullableValue<T>(Func<IDecoder, T> reader, long nullIndex) where T : struct
        {
            throw new NotImplementedException();
        }

        public string ReadString()
        {
            ReadStart();
            return Convert.ToString(_reader.Value);
        }

        public TimeSpan ReadTimeMS()
        {
            throw new NotImplementedException();
        }

        public TimeSpan ReadTimeNS()
        {
            throw new NotImplementedException();
        }

        public DateTime ReadTimestampMS()
        {
            throw new NotImplementedException();
        }

        public DateTime ReadTimestampNS()
        {
            throw new NotImplementedException();
        }

        public DateTime ReadTimestampUS()
        {
            throw new NotImplementedException();
        }

        public TimeSpan ReadTimeUS()
        {
            throw new NotImplementedException();
        }

        public Guid ReadUuid()
        {
            throw new NotImplementedException();
        }

        public void SkipArray(Action<IDecoder> itemsSkipper)
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

        public void SkipMap(Action<IDecoder> valuesSkipper)
        {
            throw new NotImplementedException();
        }

        public void SkipNull()
        {
            throw new NotImplementedException();
        }

        public void SkipNullable(Action<IDecoder> skipper, long nullIndex)
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

        public void Dispose() { }
    }
}
