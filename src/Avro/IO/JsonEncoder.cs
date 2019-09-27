using Avro.IO.Formatting;
using Avro.Types;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Avro.IO
{
    public class JsonEncoder : IAvroEncoder
    {
        private readonly TextWriter _stream;
        private readonly JsonTextWriter _writer;
        private readonly string _delimiter;
        private readonly AvroJsonFormatter.Writer[] _actions;
        private int _index = 0;

        public JsonEncoder(TextWriter stream, AvroSchema schema, string delimiter = null)
        {
            _stream = stream;
            _writer = new JsonTextWriter(_stream);
            _delimiter = delimiter;
            _actions = AvroJsonFormatter.GetWriteActions(schema);
        }

        public void WriteArray<T>(IList<T> items, Action<IAvroEncoder, T> itemsWriter)
        {
            var end = Advance();
            var loop = _index;
            foreach (var item in items)
            {
                _index = loop;
                itemsWriter.Invoke(this, item);
            }
            _index = end;
            Advance();
        }

        public void WriteArrayBlock<T>(IList<T> items, Action<IAvroEncoder, T> itemsWriter)
        {
            throw new NotSupportedException();
        }

        public void WriteBoolean(bool value)
        {
            Advance(false, value.ToString());
        }

        public void WriteBytes(byte[] value)
        {
            var sb = new StringBuilder();
            foreach (var b in value)
            {
                sb.Append("\\u00");
                sb.Append(b.ToString("X2"));
            }
            Advance(true, sb.ToString());
        }

        public void WriteDate(DateTime value)
        {
            Advance(true, value.ToString());
        }

        public void WriteDecimal(decimal value, int scale)
        {
            Advance(false, value.ToString());
        }

        public void WriteDecimal(decimal value, int scale, int len)
        {
            Advance(false, value.ToString());
        }

        public void WriteDouble(double value)
        {
            Advance(false, value.ToString());
        }

        public void WriteDuration(AvroDuration value)
        {
            var bytes = new byte[12];

            bytes[0] = (byte)((value.Months >> 24) & 0xFF);
            bytes[1] = (byte)((value.Months >> 16) & 0xFF);
            bytes[2] = (byte)((value.Months >> 8) & 0xFF);
            bytes[3] = (byte)((value.Months) & 0xFF);

            bytes[4] = (byte)((value.Days >> 24) & 0xFF);
            bytes[5] = (byte)((value.Days >> 16) & 0xFF);
            bytes[6] = (byte)((value.Days >> 8) & 0xFF);
            bytes[7] = (byte)((value.Days) & 0xFF);

            bytes[8] = (byte)((value.MilliSeconds >> 24) & 0xFF);
            bytes[9] = (byte)((value.MilliSeconds >> 16) & 0xFF);
            bytes[10] = (byte)((value.MilliSeconds >> 8) & 0xFF);
            bytes[11] = (byte)((value.MilliSeconds) & 0xFF);

            var sb = new StringBuilder();
            foreach (var b in bytes)
            {
                sb.Append("\\u00");
                sb.Append(b.ToString("x2"));
            }
            Advance(true, sb.ToString());
        }

        public void WriteFixed(byte[] value)
        {
            var sb = new StringBuilder();
            foreach (var b in value)
            {
                sb.Append("\\u00");
                sb.Append(b.ToString("x2"));
            }
            Advance(true, sb.ToString());
        }

        public void WriteFloat(float value)
        {
            Advance(false, value.ToString());
        }

        public void WriteInt(int value)
        {
            Advance(false, value.ToString());
        }

        public void WriteLong(long value)
        {
            Advance(false, value.ToString());
        }

        public void WriteMap<T>(IDictionary<string, T> keyValues, Action<IAvroEncoder, T> valuesWriter)
        {
            var end = Advance();
            var loop = _index;
            foreach (var keyValue in keyValues)
            {
                _index = loop;
                _writer.WritePropertyName(keyValue.Key);
                valuesWriter.Invoke(this, keyValue.Value);
            }
            _index = end;
            Advance();
        }

        public void WriteMapBlock<T>(IDictionary<string, T> keyValues, Action<IAvroEncoder, T> valuesWriter)
        {
            throw new NotSupportedException();
        }

        public void WriteNull(AvroNull value)
        {
            Advance();
        }

        public void WriteNullableObject<T>(T value, Action<IAvroEncoder, T> valueWriter, long nullIndex) where T : class
        {
            if (value == null)
            {
                Advance(false, nullIndex.ToString());
                Advance();
            }
            else
            {
                Advance(false, ((nullIndex + 1) % 2).ToString());
                valueWriter.Invoke(this, value);
            }
        }

        public void WriteNullableValue<T>(T? value, Action<IAvroEncoder, T> valueWriter, long nullIndex) where T : struct
        {
            if (value.HasValue)
            {
                Advance(false, ((nullIndex + 1) % 2).ToString());
                valueWriter.Invoke(this, value.Value);
            }
            else
            {
                Advance(false, nullIndex.ToString());
                Advance();
            }
        }

        public void WriteString(string value)
        {
            Advance(true, value.ToString());
        }

        public void WriteTimeMS(TimeSpan value)
        {
            Advance(true, value.ToString());
        }

        public void WriteTimeUS(TimeSpan value)
        {
            Advance(true, value.ToString());
        }

        public void WriteTimeNS(TimeSpan value)
        {
            Advance(true, value.ToString());
        }

        public void WriteTimestampMS(DateTime value)
        {
            Advance(true, value.ToString());
        }

        public void WriteTimestampUS(DateTime value)
        {
            Advance(true, value.ToString());
        }

        public void WriteTimestampNS(DateTime value)
        {
            Advance(true, value.ToString());
        }

        public void WriteUuid(Guid value)
        {
            Advance(true, value.ToString());
        }

        public void WriteUnion<T1, T2>(
            AvroUnion<T1, T2> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2
        )
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
                case 1:
                    valueWriter2.Invoke(this, value.GetT2());
                    break;
            }
        }

        public void WriteUnion<T1, T2, T3>(
            AvroUnion<T1, T2, T3> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3
        )
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
                case 1:
                    valueWriter2.Invoke(this, value.GetT2());
                    break;
                case 2:
                    valueWriter3.Invoke(this, value.GetT3());
                    break;
            }
        }

        public void WriteUnion<T1, T2, T3, T4>(
            AvroUnion<T1, T2, T3, T4> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4
        )
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
                case 1:
                    valueWriter2.Invoke(this, value.GetT2());
                    break;
                case 2:
                    valueWriter3.Invoke(this, value.GetT3());
                    break;
                case 3:
                    valueWriter4.Invoke(this, value.GetT4());
                    break;
            }
        }

        public void WriteUnion<T1, T2, T3, T4, T5>(
            AvroUnion<T1, T2, T3, T4, T5> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4,
            Action<IAvroEncoder, T5> valueWriter5
        )
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
                case 1:
                    valueWriter2.Invoke(this, value.GetT2());
                    break;
                case 2:
                    valueWriter3.Invoke(this, value.GetT3());
                    break;
                case 3:
                    valueWriter4.Invoke(this, value.GetT4());
                    break;
                case 4:
                    valueWriter5.Invoke(this, value.GetT5());
                    break;
            }
        }

        public void WriteUnion<T1, T2, T3, T4, T5, T6>(
            AvroUnion<T1, T2, T3, T4, T5, T6> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4,
            Action<IAvroEncoder, T5> valueWriter5,
            Action<IAvroEncoder, T6> valueWriter6
        )
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
                case 1:
                    valueWriter2.Invoke(this, value.GetT2());
                    break;
                case 2:
                    valueWriter3.Invoke(this, value.GetT3());
                    break;
                case 3:
                    valueWriter4.Invoke(this, value.GetT4());
                    break;
                case 4:
                    valueWriter5.Invoke(this, value.GetT5());
                    break;
                case 5:
                    valueWriter6.Invoke(this, value.GetT6());
                    break;
            }
        }

        public void WriteUnion<T1, T2, T3, T4, T5, T6, T7>(
            AvroUnion<T1, T2, T3, T4, T5, T6, T7> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4,
            Action<IAvroEncoder, T5> valueWriter5,
            Action<IAvroEncoder, T6> valueWriter6,
            Action<IAvroEncoder, T7> valueWriter7
        )
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
                case 1:
                    valueWriter2.Invoke(this, value.GetT2());
                    break;
                case 2:
                    valueWriter3.Invoke(this, value.GetT3());
                    break;
                case 3:
                    valueWriter4.Invoke(this, value.GetT4());
                    break;
                case 4:
                    valueWriter5.Invoke(this, value.GetT5());
                    break;
                case 5:
                    valueWriter6.Invoke(this, value.GetT6());
                    break;
                case 6:
                    valueWriter7.Invoke(this, value.GetT7());
                    break;
            }
        }

        public void WriteUnion<T1, T2, T3, T4, T5, T6, T7, T8>(
            AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4,
            Action<IAvroEncoder, T5> valueWriter5,
            Action<IAvroEncoder, T6> valueWriter6,
            Action<IAvroEncoder, T7> valueWriter7,
            Action<IAvroEncoder, T8> valueWriter8
        )
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
                case 1:
                    valueWriter2.Invoke(this, value.GetT2());
                    break;
                case 2:
                    valueWriter3.Invoke(this, value.GetT3());
                    break;
                case 3:
                    valueWriter4.Invoke(this, value.GetT4());
                    break;
                case 4:
                    valueWriter5.Invoke(this, value.GetT5());
                    break;
                case 5:
                    valueWriter6.Invoke(this, value.GetT6());
                    break;
                case 6:
                    valueWriter7.Invoke(this, value.GetT7());
                    break;
                case 7:
                    valueWriter8.Invoke(this, value.GetT8());
                    break;
            }
        }

        public void WriteUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(
            AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> value,
            Action<IAvroEncoder, T1> valueWriter1,
            Action<IAvroEncoder, T2> valueWriter2,
            Action<IAvroEncoder, T3> valueWriter3,
            Action<IAvroEncoder, T4> valueWriter4,
            Action<IAvroEncoder, T5> valueWriter5,
            Action<IAvroEncoder, T6> valueWriter6,
            Action<IAvroEncoder, T7> valueWriter7,
            Action<IAvroEncoder, T8> valueWriter8,
            Action<IAvroEncoder, T9> valueWriter9
        )
        {
            switch (value.Index)
            {
                case 0:
                    valueWriter1.Invoke(this, value.GetT1());
                    break;
                case 1:
                    valueWriter2.Invoke(this, value.GetT2());
                    break;
                case 2:
                    valueWriter3.Invoke(this, value.GetT3());
                    break;
                case 3:
                    valueWriter4.Invoke(this, value.GetT4());
                    break;
                case 4:
                    valueWriter5.Invoke(this, value.GetT5());
                    break;
                case 5:
                    valueWriter6.Invoke(this, value.GetT6());
                    break;
                case 6:
                    valueWriter7.Invoke(this, value.GetT7());
                    break;
                case 7:
                    valueWriter8.Invoke(this, value.GetT8());
                    break;
                case 8:
                    valueWriter9.Invoke(this, value.GetT9());
                    break;
            }
        }

        public void Dispose() { }

        private int Advance(bool quote = false, string value = "null")
        {
            if (_index >= _actions.Length)
            {
                _stream.Write(_delimiter);
                _index = 0;
            }
            return _actions[_index++].Invoke(_writer, quote, value);
        }
    }
}
