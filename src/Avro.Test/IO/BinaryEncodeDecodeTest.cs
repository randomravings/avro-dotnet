using Avro.IO;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace Avro.Test.IO
{
    [TestFixture]
    public class BinaryEncodeDecodeTest
    {
        [TestCase()]
        public void TestNull()
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteNull();
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadNull();
                Assert.AreEqual(null, actual);
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void TestBoolean(bool expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteBoolean(expected);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadBoolean();
                Assert.AreEqual(expected, actual);
            }
        }

        [TestCase(0)]
        [TestCase(1)]
        [TestCase(2)]
        [TestCase(3)]
        [TestCase(10)]
        [TestCase(1024)]
        [TestCase(2048)]
        [TestCase(200000)]
        [TestCase(-1)]
        [TestCase(-2)]
        [TestCase(-3)]
        [TestCase(-10)]
        [TestCase(-1024)]
        [TestCase(-2048)]
        [TestCase(-200000)]
        [TestCase(int.MinValue)]
        [TestCase(int.MaxValue)]
        public void TestInt(int expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteInt(expected);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadInt();
                Assert.AreEqual(expected, actual);
            }
        }

        [TestCase(0L)]
        [TestCase(1L)]
        [TestCase(2L)]
        [TestCase(3L)]
        [TestCase(10L)]
        [TestCase(1024L)]
        [TestCase(2048L)]
        [TestCase(200000L)]
        [TestCase(-1L)]
        [TestCase(-2L)]
        [TestCase(-3L)]
        [TestCase(-10L)]
        [TestCase(-1024L)]
        [TestCase(-2048L)]
        [TestCase(-200000L)]
        [TestCase(long.MinValue)]
        [TestCase(long.MaxValue)]
        public void TestLong(long expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteLong(expected);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadLong();
                Assert.AreEqual(expected, actual);
            }
        }

        [TestCase(0.0F)]
        [TestCase(1.0F)]
        [TestCase(2.0F)]
        [TestCase(3.0F)]
        [TestCase(0.123456789F)]
        [TestCase(0.00000000000000000001F)]
        [TestCase(123456789.123456789F)]
        [TestCase(234627.1111343F)]
        [TestCase(1.2e-3F)]
        [TestCase(3.14e12F)]
        [TestCase(-1.0F)]
        [TestCase(-2.0F)]
        [TestCase(-3.0F)]
        [TestCase(-0.123456789F)]
        [TestCase(-0.00000000000000000001F)]
        [TestCase(-123456789.123456789F)]
        [TestCase(-234627.1111343F)]
        [TestCase(-1.2e-3F)]
        [TestCase(-3.14e12F)]
        [TestCase(float.MinValue)]
        [TestCase(float.MaxValue)]
        public void TestFloat(float expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteFloat(expected);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadFloat();
                Assert.AreEqual(expected, actual);
            }
        }

        [TestCase(0.0D)]
        [TestCase(1.0D)]
        [TestCase(2.0D)]
        [TestCase(3.0D)]
        [TestCase(0.32459832450982340983450872345D)]
        [TestCase(0.0000000000000000000000000000001D)]
        [TestCase(31234.7633245D)]
        [TestCase(234627.1111343D)]
        [TestCase(9459043089734587324924357.08973450897324508976234D)]
        [TestCase(6.8e-63D)]
        [TestCase(9.4e89D)]
        [TestCase(-1.0D)]
        [TestCase(-2.0D)]
        [TestCase(-3.0D)]
        [TestCase(-0.32459832450982340983450872345D)]
        [TestCase(-0.0000000000000000000000000000001D)]
        [TestCase(-31234.7633245D)]
        [TestCase(-234627.1111343D)]
        [TestCase(-9459043089734587324924357.08973450897324508976234D)]
        [TestCase(-6.8e-163D)]
        [TestCase(9 - .4e289D)]
        [TestCase(double.MinValue)]
        [TestCase(double.MaxValue)]
        public void TestDouble(double expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteDouble(expected);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadDouble();
                Assert.AreEqual(expected, actual);
            }
        }

        [TestCase(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06 })]
        [TestCase(new byte[] { 0xFF, 0x00, 0x1F, 0xC6 })]
        public void TestBytes(byte[] expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteBytes(expected);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadBytes();
                Assert.AreEqual(expected, actual);
            }
        }

        [TestCase("Hello World!")]
        [TestCase("Avro is Awesome")]
        [TestCase("Some Numbers: 1234567890")]
        [TestCase("Some Symbols: !#¤%&/()=?^¨~'*_-")]
        [TestCase("abcdefghijklmnopqrstuvwxyz")]
        public void TestString(string expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteString(expected);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadString();
                Assert.AreEqual(expected, actual);
            }
        }

        [TestCase(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06 })]
        [TestCase(new byte[] { 0xFF, 0x00, 0x1F, 0xC6 })]
        public void TestFixed(byte[] expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteFixed(expected);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadFixed(new byte[expected.Length]);
                Assert.AreEqual(expected, actual);
            }
        }

        [TestCase("1977-05-25")]
        [TestCase("2019-01-01")]
        [TestCase("3407-03-16")]
        [TestCase("1234-05-06")]
        [TestCase("0001-01-01")]
        [TestCase("9999-12-31")]
        public void TestDate(string expected)
        {
            var value = DateTime.Parse(expected);
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteDate(value);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadDate();
                Assert.AreEqual(value, actual);
            }
        }

        [TestCase("0", 0)]
        [TestCase("1", 0)]
        [TestCase("-1", 0)]
        [TestCase("123.456", 6)]
        [TestCase("-7922816251426433.7593543950335", 13)]
        [TestCase("7922816251426433.7593543950335", 13)]
        public void TestDecimal(string expected, int scale)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                var expectedDecimal = decimal.Parse(expected);
                encoder.WriteDecimal(expectedDecimal, scale);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadDecimal(scale);
                Assert.AreEqual(expectedDecimal, actual);
            }
        }

        [TestCase("0", 0, 32)]
        [TestCase("1", 0, 32)]
        [TestCase("-1", 0, 32)]
        [TestCase("123.456", 6, 32)]
        [TestCase("-7922816251426433.7593543950335", 13, 32)]
        [TestCase("7922816251426433.7593543950335", 13, 32)]
        public void TestDecimalFixed(string expected, int scale, int len)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                var expectedDecimal = decimal.Parse(expected);
                encoder.WriteDecimal(expectedDecimal, scale, len);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadDecimal(scale, len);
                Assert.AreEqual(expectedDecimal, actual);
            }
        }

        [TestCase("00:00:00:00.000")]
        [TestCase("00:12:12:12.123")]
        public void TestTimeMs(string expected)
        {
            var time = TimeSpan.ParseExact(expected, @"d\:hh\:mm\:ss\.fff", CultureInfo.InvariantCulture);
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteTimeMS(time);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadTimeMS();
                Assert.AreEqual(time, actual);
            }
        }

        [TestCase("00:00:00:00.000000")]
        [TestCase("00:12:12:12.123456")]
        public void TestTimeUs(string expected)
        {
            var time = TimeSpan.ParseExact(expected, @"d\:hh\:mm\:ss\.ffffff", CultureInfo.InvariantCulture);
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteTimeUS(time);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadTimeUS();
                Assert.AreEqual(time, actual);
            }
        }

        [TestCase("00:00:00:00.0000000")]
        [TestCase("00:12:12:12.1234567")]
        public void TestTimeNs(string expected)
        {
            var time = TimeSpan.ParseExact(expected, @"d\:hh\:mm\:ss\.fffffff", CultureInfo.InvariantCulture);
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteTimeNS(time);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadTimeNS();
                Assert.AreEqual(time, actual);
            }
        }

        [TestCase("1900-01-01:00.00.00.000")]
        [TestCase("2019-03-31:12.53.12.345")]
        public void TestTimestampMs(string expected)
        {
            var time = DateTime.ParseExact(expected, "yyyy-MM-dd:HH.mm.ss.fff", CultureInfo.InvariantCulture);
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteTimestampMS(time);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadTimestampMS();
                Assert.AreEqual(time, actual);
            }
        }

        [TestCase("1900-01-01:00.00.00.000000")]
        [TestCase("2019-03-31:12.53.12.345678")]
        public void TestTimestampUs(string expected)
        {
            var time = DateTime.ParseExact(expected, "yyyy-MM-dd:HH.mm.ss.ffffff", CultureInfo.InvariantCulture);
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteTimestampUS(time);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadTimestampUS();
                Assert.AreEqual(time, actual);
            }
        }

        [TestCase(new int[] { 0, 0, 0 })]
        [TestCase(new int[] { 1, 2, 3 })]
        [TestCase(new int[] { 4, 7, 34563456 })]
        public void TestDuration(int[] expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                var expectedDuration = new ValueTuple<int, int, int>(expected[0], expected[1], expected[2]);
                encoder.WriteDuration(expectedDuration);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadDuration();
                Assert.AreEqual(expectedDuration, actual);
            }
        }

        [TestCase("00000000-0000-0000-0000-000000000000")]
        [TestCase("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF")]
        [TestCase("ad210816-e1c0-4fdc-87e5-96262229a70a")]
        [TestCase("ec0b27db-af0f-4768-a6ba-0ebf440c0bb2")]
        [TestCase("DBE9CF09-2E52-484B-A40C-2F48B1ED9630")]
        [TestCase("61FEDC68-47CA-4727-BDFF-685A4E3EC846")]
        public void TestUuid(string expected)
        {
            var uuid = Guid.Parse(expected);
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteUuid(uuid);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadUuid();
                Assert.AreEqual(uuid, actual);
            }
        }

        [Test, TestCaseSource(typeof(ArrayData))]
        public void TestArray(IList<string> expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteArray(expected, (s, e) => s.WriteString(e) );
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadArray(s => s.ReadString());
                Assert.AreEqual(expected, actual);
            }
        }

        [Test, TestCaseSource(typeof(MapData))]
        public void TestMap(IDictionary<string, int> expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteMap(expected, (s, e) => s.WriteInt(e));
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadMap(s => s.ReadInt());
                Assert.AreEqual(expected, actual);
            }
        }

        [TestCase(null)]
        [TestCase(456)]
        public void TestNullableInt(int? expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteNullableValue(expected, (s, e) => s.WriteInt(e), 1);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadNullableValue(s => s.ReadInt(), 1);
                Assert.AreEqual(expected, actual);
            }
        }

        [TestCase(null)]
        [TestCase(new byte[] { 0x01, 0x02, 0x03, 0x04 })]
        public void TestNullableBytes(byte[] expected)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            using (var decoder = new BinaryDecoder(stream))
            {
                encoder.WriteNullableObject(expected, (s, e) => s.WriteBytes(e), 1);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = decoder.ReadNullableObject(s => s.ReadBytes(), 1);
                Assert.AreEqual(expected, actual);
            }
        }

        class ArrayData : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new List<string>();
                yield return new List<string>() { "String xyz", "Some other string" };
            }
        }

        class MapData : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new Dictionary<string, int>();
                yield return new Dictionary<string, int>() { { "Key1", 234 }, {"MaxInt", int.MaxValue }, { "", 0 } };
            }
        }
    }
}
