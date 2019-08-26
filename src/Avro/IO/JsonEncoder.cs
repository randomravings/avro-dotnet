using Avro.IO.Formatting;
using Avro.Types;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Avro.IO
{
    public class JsonEncoder : IEncoder
    {
        private readonly TextWriter _stream;
        private readonly AvroSchema _schema;
        private readonly string _delimiter;
        private readonly JsonTextWriter _writer;
        private readonly IList<JsonItemDescriptor> _headings;

        private int _index = 0;
        private Stack<JsonItemDescriptor> _itemStack;

        public JsonEncoder(TextWriter stream, AvroSchema schema, string delimiter = null)
        {
            _stream = stream;
            _schema = schema;
            _delimiter = delimiter;
            _writer = new JsonTextWriter(_stream);
            _headings = new List<JsonItemDescriptor>();
            _itemStack = new Stack<JsonItemDescriptor>();
            JsonItemDescriptor.AddHeading(_schema, _headings);
        }        

        private void WriteStart()
        {
            if (_index == 0)
                _writer.WriteStartObject();
            var item = _headings[_index++];
            _itemStack.Push(item);
            _writer.WritePropertyName(item.PropertyName);
            if (item.IsRecord)
            {
                _writer.WriteStartObject();
                WriteStart();
            }
        }

        private void WriteEnd()
        {
            _itemStack.Pop();
            if (_itemStack.Count == 0)
            {
                _writer.WriteEndObject();
                _stream.Write(_delimiter);
                _index = 0;
                return;
            }
            var item = _itemStack.Peek();
            if (item.IsRecord && _index >= item.Limit)
            {
                _writer.WriteEndObject();
                WriteEnd();
            }
        }

        public void WriteArray<T>(IList<T> items, Action<IEncoder, T> itemsWriter)
        {
            WriteStart();
            _writer.WriteStartArray();
            var loop = _index;
            foreach (var item in items)
            {
                _writer.WriteStartObject();
                _index = loop;
                itemsWriter.Invoke(this, item);
                _writer.WriteEndObject();
            }
            _index = _itemStack.Peek().Limit;
            _writer.WriteEndArray();
            WriteEnd();
        }

        public void WriteArrayBlock<T>(IList<T> items, Action<IEncoder, T> itemsWriter)
        {
            throw new NotSupportedException();
        }

        public void WriteBoolean(bool value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteBytes(byte[] value)
        {
            WriteStart();
            var sb = new StringBuilder();
            foreach (var b in value)
            {
                sb.Append("\\u00");
                sb.Append(b.ToString("X2"));
            }
            _writer.WriteValue(sb.ToString());
            WriteEnd();
        }

        public void WriteDate(DateTime value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteDecimal(decimal value, int scale)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteDecimal(decimal value, int scale, int len)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteDouble(double value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteDuration(AvroDuration value)
        {
            WriteStart();
            var bytes = new byte[12];

            bytes[0] = ((byte)((value.Months >> 24) & 0xFF));
            bytes[1] = ((byte)((value.Months >> 16) & 0xFF));
            bytes[2] = ((byte)((value.Months >> 8) & 0xFF));
            bytes[3] = ((byte)((value.Months) & 0xFF));

            bytes[4] = ((byte)((value.Days >> 24) & 0xFF));
            bytes[5] = ((byte)((value.Days >> 16) & 0xFF));
            bytes[6] = ((byte)((value.Days >> 8) & 0xFF));
            bytes[7] = ((byte)((value.Days) & 0xFF));

            bytes[8] = ((byte)((value.MilliSeconds >> 24) & 0xFF));
            bytes[9] = ((byte)((value.MilliSeconds >> 16) & 0xFF));
            bytes[10] = ((byte)((value.MilliSeconds >> 8) & 0xFF));
            bytes[11] = ((byte)((value.MilliSeconds) & 0xFF));

            var sb = new StringBuilder();
            foreach (var b in bytes)
            {
                sb.Append("\\u00");
                sb.Append(b.ToString("x2"));
            }
            _writer.WriteValue(sb.ToString());
            WriteEnd();
        }

        public void WriteFixed(byte[] value)
        {
            WriteStart();
            var sb = new StringBuilder();
            foreach (var b in value)
            {
                sb.Append("\\u00");
                sb.Append(b.ToString("x2"));
            }
            _writer.WriteValue(sb.ToString());
            WriteEnd();
        }

        public void WriteFloat(float value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteInt(int value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteLong(long value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteMap<T>(IDictionary<string, T> keyValues, Action<IEncoder, T> valuesWriter)
        {
            WriteStart();
            _writer.WriteStartObject();
            var loop = _index;
            foreach (var keyValue in keyValues)
            {
                _index = loop;
                _writer.WritePropertyName(keyValue.Key);
                _writer.WriteStartObject();
                valuesWriter.Invoke(this, keyValue.Value);
                _writer.WriteEndObject();
            }
            _index = _itemStack.Peek().Limit;
            _writer.WriteEndObject();
            WriteEnd();
        }

        public void WriteMapBlock<T>(IDictionary<string, T> keyValues, Action<IEncoder, T> valuesWriter)
        {
            throw new NotSupportedException();
        }

        public void WriteNull()
        {
            WriteStart();
            _writer.WriteNull();
            WriteEnd();
        }

        public void WriteNullableObject<T>(T value, Action<IEncoder, T> valueWriter, long nullIndex) where T : class
        {
            WriteStart();
            if (value == null)
                _writer.WriteNull();
            else
                valueWriter.Invoke(this, value);
            WriteEnd();
        }

        public void WriteNullableValue<T>(T? value, Action<IEncoder, T> valueWriter, long nullIndex) where T : struct
        {
            WriteStart();
            if (value.HasValue)
                valueWriter.Invoke(this, value.Value);
            else
                _writer.WriteNull();
            WriteEnd();
        }

        public void WriteString(string value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteTimeMS(TimeSpan value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteTimeUS(TimeSpan value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteTimeNS(TimeSpan value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteTimestampMS(DateTime value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteTimestampUS(DateTime value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteTimestampNS(DateTime value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void WriteUuid(Guid value)
        {
            WriteStart();
            _writer.WriteValue(value);
            WriteEnd();
        }

        public void Dispose() { }
    }
}
