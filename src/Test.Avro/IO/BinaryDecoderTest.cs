using Avro.IO;
using Avro.Types;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace Test.Avro.IO
{
    [TestFixture]
    public class BinaryDecoderTest
    {
        [TestCase()]
        public void DecodeNull()
        {
            var expectedValue = AvroNull.Value;
            using var stream = new MemoryStream(new byte[] { });
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadNull();
            Assert.AreEqual(0, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipNull();
            Assert.AreEqual(0, stream.Position, "Skip offset error");
        }

        [TestCase(false, 1, new byte[] { 0x00 })]
        [TestCase(true, 1, new byte[] { 0x01 })]
        public void DecodeBoolean(bool expectedValue, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadBoolean();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipBoolean();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase(0, 1, new byte[] { 0x0 })]
        [TestCase(1, 1, new byte[] { 0x2 })]
        [TestCase(2, 1, new byte[] { 0x4 })]
        [TestCase(3, 1, new byte[] { 0x6 })]
        [TestCase(4, 1, new byte[] { 0x8 })]
        [TestCase(5, 1, new byte[] { 0xA })]
        [TestCase(6, 1, new byte[] { 0xC })]
        [TestCase(7, 1, new byte[] { 0xE })]
        [TestCase(8, 1, new byte[] { 0x10 })]
        [TestCase(9, 1, new byte[] { 0x12 })]
        [TestCase(10, 1, new byte[] { 0x14 })]
        [TestCase(63, 1, new byte[] { 0x7E })]
        [TestCase(64, 2, new byte[] { 0x80, 0x01 })]
        [TestCase(8191, 2, new byte[] { 0xFE, 0x7F })]
        [TestCase(8192, 3, new byte[] { 0x80, 0x80, 0x01 })]
        [TestCase(1048575, 3, new byte[] { 0xFE, 0xFF, 0x7F })]
        [TestCase(1048576, 4, new byte[] { 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(134217727, 4, new byte[] { 0xFE, 0xFF, 0xFF, 0x7F })]
        [TestCase(134217728, 5, new byte[] { 0x80, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(int.MinValue, 5, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x0F })]
        [TestCase(-1, 1, new byte[] { 0x1 })]
        [TestCase(-2, 1, new byte[] { 0x3 })]
        [TestCase(-3, 1, new byte[] { 0x5 })]
        [TestCase(-4, 1, new byte[] { 0x7 })]
        [TestCase(-5, 1, new byte[] { 0x9 })]
        [TestCase(-6, 1, new byte[] { 0xB })]
        [TestCase(-7, 1, new byte[] { 0xD })]
        [TestCase(-8, 1, new byte[] { 0xF })]
        [TestCase(-9, 1, new byte[] { 0x11 })]
        [TestCase(-10, 1, new byte[] { 0x13 })]
        [TestCase(-64, 1, new byte[] { 0x7F })]
        [TestCase(-65, 2, new byte[] { 0x81, 0x01 })]
        [TestCase(-8192, 2, new byte[] { 0xFF, 0x7F })]
        [TestCase(-8193, 3, new byte[] { 0x81, 0x80, 0x01 })]
        [TestCase(-1048576, 3, new byte[] { 0xFF, 0xFF, 0x7F })]
        [TestCase(-1048577, 4, new byte[] { 0x81, 0x80, 0x80, 0x01 })]
        [TestCase(-134217728, 4, new byte[] { 0xFF, 0xFF, 0xFF, 0x7F })]
        [TestCase(-134217729, 5, new byte[] { 0x81, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(int.MaxValue, 5, new byte[] { 0xFE, 0xFF, 0xFF, 0xFF, 0x0F })]
        public void DecodeInt(int expectedValue, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadInt();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipInt();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase(0L, 1, new byte[] { 0x0 })]
        [TestCase(1L, 1, new byte[] { 0x2 })]
        [TestCase(2L, 1, new byte[] { 0x4 })]
        [TestCase(3L, 1, new byte[] { 0x6 })]
        [TestCase(4L, 1, new byte[] { 0x8 })]
        [TestCase(5L, 1, new byte[] { 0xA })]
        [TestCase(6L, 1, new byte[] { 0xC })]
        [TestCase(7L, 1, new byte[] { 0xE })]
        [TestCase(8L, 1, new byte[] { 0x10 })]
        [TestCase(9L, 1, new byte[] { 0x12 })]
        [TestCase(10L, 1, new byte[] { 0x14 })]
        [TestCase(63L, 1, new byte[] { 0x7E })]
        [TestCase(64L, 2, new byte[] { 0x80, 0x01 })]
        [TestCase(8191L, 2, new byte[] { 0xFE, 0x7F })]
        [TestCase(8192L, 3, new byte[] { 0x80, 0x80, 0x01 })]
        [TestCase(1048575L, 3, new byte[] { 0xFE, 0xFF, 0x7F })]
        [TestCase(1048576L, 4, new byte[] { 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(134217727L, 4, new byte[] { 0xFE, 0xFF, 0xFF, 0x7F })]
        [TestCase(134217728L, 5, new byte[] { 0x80, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(17179869183L, 5, new byte[] { 0xFE, 0xFF, 0xFF, 0xFF, 0x7F })]
        [TestCase(17179869184L, 6, new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(2199023255551L, 6, new byte[] { 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F })]
        [TestCase(2199023255552L, 7, new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(281474976710655L, 7, new byte[] { 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F })]
        [TestCase(281474976710656L, 8, new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(36028797018963967L, 8, new byte[] { 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F })]
        [TestCase(36028797018963968L, 9, new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(4611686018427387903L, 9, new byte[] { 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F })]
        [TestCase(4611686018427387904L, 10, new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(9223372036854775807L, 10, new byte[] { 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01 })]
        [TestCase(-1L, 1, new byte[] { 0x1 })]
        [TestCase(-2L, 1, new byte[] { 0x3 })]
        [TestCase(-3L, 1, new byte[] { 0x5 })]
        [TestCase(-4L, 1, new byte[] { 0x7 })]
        [TestCase(-5L, 1, new byte[] { 0x9 })]
        [TestCase(-6L, 1, new byte[] { 0xB })]
        [TestCase(-7L, 1, new byte[] { 0xD })]
        [TestCase(-8L, 1, new byte[] { 0xF })]
        [TestCase(-9L, 1, new byte[] { 0x11 })]
        [TestCase(-10L, 1, new byte[] { 0x13 })]
        [TestCase(-64L, 1, new byte[] { 0x7F })]
        [TestCase(-65L, 2, new byte[] { 0x81, 0x01 })]
        [TestCase(-8192L, 2, new byte[] { 0xFF, 0x7F })]
        [TestCase(-8193L, 3, new byte[] { 0x81, 0x80, 0x01 })]
        [TestCase(-1048576L, 3, new byte[] { 0xFF, 0xFF, 0x7F })]
        [TestCase(-1048577L, 4, new byte[] { 0x81, 0x80, 0x80, 0x01 })]
        [TestCase(-134217728L, 4, new byte[] { 0xFF, 0xFF, 0xFF, 0x7F })]
        [TestCase(-134217729L, 5, new byte[] { 0x81, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(-17179869184L, 5, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x7F })]
        [TestCase(-17179869185L, 6, new byte[] { 0x81, 0x80, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(-2199023255552L, 6, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F })]
        [TestCase(-2199023255553L, 7, new byte[] { 0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(-281474976710656L, 7, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F })]
        [TestCase(-281474976710657L, 8, new byte[] { 0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(-36028797018963968L, 8, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F })]
        [TestCase(-36028797018963969L, 9, new byte[] { 0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(-4611686018427387904L, 9, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F })]
        [TestCase(-4611686018427387905L, 10, new byte[] { 0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 })]
        [TestCase(long.MinValue, 10, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01 })]
        public void DecodeLong(long expectedValue, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadLong();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipLong();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase(0, 4, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(-0.0012F, 4, new byte[] { 0x52, 0x49, 0x9D, 0xBA })]
        [TestCase(9.12e12F, 4, new byte[] { 0xA6, 0xB6, 0x04, 0x55 })]
        [TestCase(float.MinValue, 4, new byte[] { 0xFF, 0xFF, 0x7F, 0xFF })]
        [TestCase(float.MaxValue, 4, new byte[] { 0xFF, 0xFF, 0x7F, 0x7F })]
        public void DecodeFloat(float expectedValue, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadFloat();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipFloat();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase(0, 8, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(-89734587324924357.089734508973244D, 8, new byte[] { 0x1C, 0x18, 0x99, 0x1B, 0xD2, 0xEC, 0x73, 0xC3 })]
        [TestCase(9.12e12F, 8, new byte[] { 0x00, 0x00, 0x00, 0xC0, 0xD4, 0x96, 0xA0, 0x42 })]
        [TestCase(double.MinValue, 8, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEF, 0xFF })]
        [TestCase(double.MaxValue, 8, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEF, 0x7F })]
        public void DecodeDouble(double expectedValue, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadDouble();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipDouble();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase("Hello World!", 13, new byte[] { 0x18, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21 })]
        [TestCase("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 27, new byte[] { 0x34, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A })]
        [TestCase("zyxwvutsrqponmlkjihgfedcba", 27, new byte[] { 0x34, 0x7A, 0x79, 0x78, 0x77, 0x76, 0x75, 0x74, 0x73, 0x72, 0x71, 0x70, 0x6F, 0x6E, 0x6D, 0x6C, 0x6B, 0x6A, 0x69, 0x68, 0x67, 0x66, 0x65, 0x64, 0x63, 0x62, 0x61 })]
        [TestCase(" !#$%&'()*+,-./0123456789:;<=>?@", 33, new byte[] { 0x40, 0x20, 0x21, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F, 0x40 })]
        public void DecodeString(string expectedValue, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadString();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipString();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase(new byte[] { }, 1, new byte[] { 0x00 })]
        [TestCase(new byte[] { 0x01, 0x01, 0x01, 0x01, 0x01 }, 6, new byte[] { 0x0A, 0x01, 0x01, 0x01, 0x01, 0x01 })]
        public void DecodeBytes(byte[] expectedValue, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadBytes();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipBytes();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase(new byte[] { }, 0, new byte[] { })]
        [TestCase(new byte[] { 0x01, 0x02, 0x03, 0x04 }, 4, new byte[] { 0x01, 0x02, 0x03, 0x04 })]
        public void DecodeFixed(byte[] expectedValue, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadFixed(expectedValue.Length);
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipFixed(expectedValue.Length);
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [Test, TestCaseSource(typeof(ArrayItemData))]
        public void DecodeArray(IList<int> expectedValue, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadArray(s => s.ReadInt());
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipArray(s => s.SkipInt());
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [Test, TestCaseSource(typeof(ArrayBlockData))]
        public void DecodeArrayBlock(IList<IList<int>> expectedValue, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = new List<IList<int>>();
            var loop = true;
            while (loop)
            {
                var block = new List<int>();
                loop = decoder.ReadArrayBlock(s => s.ReadInt(), ref block);
                decode.Add(block);
            }
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipArray(s => s.SkipInt());
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [Test, TestCaseSource(typeof(MapItemData))]
        public void DecodeMap(IDictionary<string, int> expectedValue, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadMap(s => s.ReadInt());
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipMap(s => s.SkipInt());
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [Test, TestCaseSource(typeof(MapBlockData))]
        public void DecodeMapBlock(IList<IDictionary<string, int>> expectedValue, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = new List<IDictionary<string, int>>();
            var loop = true;
            while (loop)
            {
                var block = new Dictionary<string, int>();
                loop = decoder.ReadMapBlock(s => s.ReadInt(), ref block);
                decode.Add(block);
            }
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipMap(s => s.SkipInt());
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase("1977-05-25", 2, new byte[] { 0x9A, 0x2A })]
        [TestCase("0001-01-01", 3, new byte[] { 0xF3, 0xE4, 0x57 })]
        [TestCase("9999-12-31", 4, new byte[] { 0xC0, 0x82, 0xE6, 0x02 })]
        public void DecodeDate(string expectedValue, int expectedLength, byte[] value)
        {
            var dateValue = DateTime.Parse(expectedValue);
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadDate();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(dateValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipDate();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase("0", 0, 2, new byte[] { 0x02, 0x00 })]
        [TestCase("1", 0, 2, new byte[] { 0x02, 0x01 })]
        [TestCase("-1", 0, 2, new byte[] { 0x02, 0xFF })]
        [TestCase("12345.06789", 5, 5, new byte[] { 0x08, 0x49, 0x95, 0x14, 0x25 })]
        [TestCase("-12345.06789", 5, 5, new byte[] { 0x08, 0xB6, 0x6A, 0xEB, 0xDB })]
        [TestCase("-7922816251426433.7593543950335", 13, 14, new byte[] { 0x1A, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 })]
        [TestCase("7922816251426433.7593543950335", 13, 14, new byte[] { 0x1A, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        public void DecodeDecimal(string expectedValue, int scale, int expectedLength, byte[] value)
        {
            var expectedDecimalValue = decimal.Parse(expectedValue);
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadDecimal(scale);
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedDecimalValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipDecimal();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase("0", 0, 16, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase("1", 0, 16, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 })]
        [TestCase("-1", 0, 16, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase("12345.06789", 5, 16, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x49, 0x95, 0x14, 0x25 })]
        [TestCase("-12345.06789", 5, 16, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xB6, 0x6A, 0xEB, 0xDB })]
        [TestCase("-7922816251426433.7593543950335", 13, 16, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 })]
        [TestCase("7922816251426433.7593543950335", 13, 16, new byte[] { 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        public void DecodeDecimalFixed(string expectedValue, int scale, int expectedLength, byte[] value)
        {
            var expectedDecimalValue = decimal.Parse(expectedValue);
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadDecimal(scale, expectedLength);
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedDecimalValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipDecimal(expectedLength);
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase("00:00:00:00.000", 1, new byte[] { 0x00 })]
        [TestCase("00:12:12:12.123", 4, new byte[] { 0xB6, 0xE7, 0xF2, 0x29 })]
        public void DecodeTimeMS(string expectedValue, int expectedLength, byte[] value)
        {
            var time = TimeSpan.ParseExact(expectedValue, @"d\:hh\:mm\:ss\.fff", CultureInfo.InvariantCulture);
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadTimeMS();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(time, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipTimeMS();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase("00:00:00:00.000000", 1, new byte[] { 0x00 })]
        [TestCase("00:12:12:12.123456", 6, new byte[] { 0x80, 0x85, 0xF8, 0xA8, 0xC7, 0x02 })]
        public void DecodeTimeUS(string expectedValue, int expectedLength, byte[] value)
        {
            var time = TimeSpan.ParseExact(expectedValue, @"d\:hh\:mm\:ss\.ffffff", CultureInfo.InvariantCulture);
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadTimeUS();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(time, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipTimeUS();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase("00:00:00:00.0000000", 1, new byte[] { 0x00 })]
        [TestCase("00:12:12:12.1234567", 6, new byte[] { 0x8E, 0xB2, 0xB0, 0x99, 0xC9, 0x19 })]
        public void DecodeTimeNS(string expectedValue, int expectedLength, byte[] value)
        {
            var time = TimeSpan.ParseExact(expectedValue, @"d\:hh\:mm\:ss\.fffffff", CultureInfo.InvariantCulture);
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadTimeNS();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(time, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipTimeNS();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase("1970-01-01:00.00.00.000", 1, new byte[] { 0x00 })]
        [TestCase("2019-03-31:12.53.12.345", 6, new byte[] { 0xB2, 0xF0, 0xFC, 0xBC, 0xBA, 0x5A })]
        public void DecodeTimestampMS(string expectedValue, int expectedLength, byte[] value)
        {
            var time = DateTime.ParseExact(expectedValue, "yyyy-MM-dd:HH.mm.ss.fff", CultureInfo.InvariantCulture);
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadTimestampMS();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(time, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipTimestampMS();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase("1970-01-01:00.00.00.000000", 1, new byte[] { 0x00 })]
        [TestCase("2019-03-31:12.53.12.345678", 8, new byte[] { 0x9C, 0x91, 0xCE, 0xAF, 0xEC, 0xD8, 0xC2, 0x05 })]
        public void DecodeTimestampUS(string expectedValue, int expectedLength, byte[] value)
        {
            var time = DateTime.ParseExact(expectedValue, "yyyy-MM-dd:HH.mm.ss.ffffff", CultureInfo.InvariantCulture);
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadTimestampUS();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(time, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipTimestampUS();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase("1970-01-01:00.00.00.0000000", 1, new byte[] { 0x00 })]
        [TestCase("2019-03-31:12.53.12.3456789", 8, new byte[] { 0xAA, 0xAC, 0x8D, 0xDC, 0xBB, 0xF8, 0x9A, 0x37 })]
        public void DecodeTimestampNS(string expectedValue, int expectedLength, byte[] value)
        {
            var time = DateTime.ParseExact(expectedValue, "yyyy-MM-dd:HH.mm.ss.fffffff", CultureInfo.InvariantCulture);
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadTimestampNS();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(time, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipTimestampNS();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase(new uint[] { 0U, 0U, 0U }, 12, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(new uint[] { 1U, 2U, 3U }, 12, new byte[] { 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03 })]
        [TestCase(new uint[] { 4U, 7U, 34563456U }, 12, new byte[] { 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x07, 0x02, 0x0F, 0x65, 0x80 })]
        public void DecodeDuration(uint[] expectedValue, int expectedLength, byte[] value)
        {
            var duration = new AvroDuration(expectedValue[0], expectedValue[1], expectedValue[2]);
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadDuration();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(duration, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipDuration();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase("00000000-0000-0000-0000-000000000000", 37, new byte[] { 0x48,
                                                                           0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x2D,
                                                                           0x30, 0x30, 0x30, 0x30, 0x2D,
                                                                           0x30, 0x30, 0x30, 0x30, 0x2D,
                                                                           0x30, 0x30, 0x30, 0x30, 0x2D,
                                                                           0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30 })]
        [TestCase("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF", 37, new byte[] { 0x48,
                                                                           0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x2D,
                                                                           0x66, 0x66, 0x66, 0x66, 0x2D,
                                                                           0x66, 0x66, 0x66, 0x66, 0x2D,
                                                                           0x66, 0x66, 0x66, 0x66, 0x2D,
                                                                           0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66 })]
        [TestCase("ad210816-e1c0-4fdc-87e5-96262229a70a", 37, new byte[] { 0x48,
                                                                           0x61, 0x64, 0x32, 0x31, 0x30, 0x38, 0x31, 0x36, 0x2D,
                                                                           0x65, 0x31, 0x63, 0x30, 0x2D,
                                                                           0x34, 0x66, 0x64, 0x63, 0x2D,
                                                                           0x38, 0x37, 0x65, 0x35, 0x2D,
                                                                           0x39, 0x36, 0x32, 0x36, 0x32, 0x32, 0x32, 0x39, 0x61, 0x37, 0x30, 0x61})]
        [TestCase("61FEDC68-47CA-4727-BDFF-685A4E3EC846", 37, new byte[] { 0x48,
                                                                           0x36, 0x31, 0x66, 0x65, 0x64, 0x63, 0x36, 0x38, 0x2D,
                                                                           0x34, 0x37, 0x63, 0x61, 0x2D,
                                                                           0x34, 0x37, 0x32, 0x37, 0x2D,
                                                                           0x62, 0x64, 0x66, 0x66, 0x2D,
                                                                           0x36, 0x38, 0x35, 0x61, 0x34, 0x65, 0x33, 0x65, 0x63, 0x38, 0x34, 0x36})]
        public void DecodeUuid(string expectedValue, int expectedLength, byte[] value)
        {
            var uuid = Guid.Parse(expectedValue);
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadUuid();
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(uuid, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipUuid();
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase(null, 0, 1, new byte[] { 0x00 })]
        [TestCase(456, 0, 3, new byte[] { 0x02, 0x90, 0x07 })]
        public void DecodeNullableValue(int? expectedValue, int expectedNullIndex, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadNullableValue(s => s.ReadInt(), expectedNullIndex);
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipNullable(s => s.SkipInt(), expectedNullIndex);
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        [TestCase(null, 1, 1, new byte[] { 0x02 })]
        [TestCase(new byte[] { 0xFF, 0xEE, 0xDD, 0xCC }, 1, 6, new byte[] { 0x00, 0x08, 0xFF, 0xEE, 0xDD, 0xCC })]
        public void DecodeNullableObject(byte[] expectedValue, int expectedNullIndex, int expectedLength, byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var decoder = new BinaryDecoder(stream);

            var decode = decoder.ReadNullableObject(s => s.ReadBytes(), expectedNullIndex);
            Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
            Assert.AreEqual(expectedValue, decode);

            stream.Seek(0, SeekOrigin.Begin);
            decoder.SkipNullable(s => s.SkipBytes(), expectedNullIndex);
            Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
        }

        class ArrayItemData : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[] { new List<int>(), 1, new byte[] { 0x00 } };
                yield return new object[] { new List<int>() { 0, 1, 2, 3, 4, 5 }, 8, new byte[] { 0x0C, 0x00, 0x02, 0x04, 0x06, 0x08, 0x0A, 0x00 } };
                yield return new object[]
                {
                    new List<int>() { 5, 4, 3, 2, 1, 0, 0, 1, 2, 3, 4, 5 },
                    17,
                    new byte[]
                    {
                        0x0D,
                        0x0C, 0x0A, 0x08, 0x06, 0x04, 0x02, 0x00,
                        0x0D,
                        0x0C, 0x00, 0x02, 0x04, 0x06, 0x08, 0x0A,
                        0x00
                    }
                };
            }
        }

        public class ArrayBlockData : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[]
{
                    new List<IList<int>>() { new List<int>() }, 1, new byte[] { 0x00 }
};
                yield return new object[]
                {
                    new List<IList<int>>() {
                        new List<int>() { 5, 4, 3, 2, 1, 0 },
                        new List<int>() { 0, 1, 2, 3, 4, 5 }
                    },
                    17,
                    new byte[]
                    {
                        0x0D,
                        0x0C, 0x0A, 0x08, 0x06, 0x04, 0x02, 0x00,
                        0x0D,
                        0x0C, 0x00, 0x02, 0x04, 0x06, 0x08, 0x0A,
                        0x00
                    }
                };
            }
        }

        class MapItemData : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                yield return new object[]
                {
                    new Dictionary<string, int>() { }, 1, new byte[] { 0x00 }
                };
                yield return new object[] { new Dictionary<string, int>() { { "Key0", 0 }, { "Key1", 1 }, { "Key2", 2 }, { "Key3", 3 }, { "Key4", 4 } }, 32,
                    new byte[]
                    {
                        0x0A,
                        0x08, 0x4B, 0x65, 0x79, 0x30,
                        0x00,
                        0x08, 0x4B, 0x65, 0x79, 0x31,
                        0x02,
                        0x08, 0x4B, 0x65, 0x79, 0x32,
                        0x04,
                        0x08, 0x4B, 0x65, 0x79, 0x33,
                        0x06,
                        0x08, 0x4B, 0x65, 0x79, 0x34,
                        0x08,
                        0x00
                    }
                };
                yield return new object[] {
                    new Dictionary<string, int>() {
                        { "Key0", 4 }, { "Key1", 3 }, { "Key2", 2 }, { "Key3", 1 }, { "Key4", 0 }, { "Key5", 0 }, { "Key6", 1 }, { "Key7", 2 }, { "Key8", 3 }, { "Key9", 4 }
                    },
                    65,
                    new byte[]
                    {
                        0x3D,
                        0x0A,

                        0x08, 0x4B, 0x65, 0x79, 0x30,
                        0x08,
                        0x08, 0x4B, 0x65, 0x79, 0x31,
                        0x06,
                        0x08, 0x4B, 0x65, 0x79, 0x32,
                        0x04,
                        0x08, 0x4B, 0x65, 0x79, 0x33,
                        0x02,
                        0x08, 0x4B, 0x65, 0x79, 0x34,
                        0x00,

                        0x3D,
                        0x0A,

                        0x08, 0x4B, 0x65, 0x79, 0x35,
                        0x00,
                        0x08, 0x4B, 0x65, 0x79, 0x36,
                        0x02,
                        0x08, 0x4B, 0x65, 0x79, 0x37,
                        0x04,
                        0x08, 0x4B, 0x65, 0x79, 0x38,
                        0x06,
                        0x08, 0x4B, 0x65, 0x79, 0x39,
                        0x08,

                        0x00
                    }
                };
            }
        }

        class MapBlockData : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {

                yield return new object[]
                {
                    new List<IDictionary<string, int>>() { new Dictionary<string, int>() }, 1, new byte[] { 0x00 }
                };
                yield return new object[] {
                    new List<IDictionary<string, int>>()
                    {
                        new Dictionary<string, int>() { { "Key0", 4 }, { "Key1", 3 }, { "Key2", 2 }, { "Key3", 1 }, { "Key4", 0 } },
                        new Dictionary<string, int>() { { "Key5", 0 }, { "Key6", 1 }, { "Key7", 2 }, { "Key8", 3 }, { "Key9", 4 } }
                    },
                    65,
                    new byte[]
                    {
                        0x3D,
                        0x0A,

                        0x08, 0x4B, 0x65, 0x79, 0x30,
                        0x08,
                        0x08, 0x4B, 0x65, 0x79, 0x31,
                        0x06,
                        0x08, 0x4B, 0x65, 0x79, 0x32,
                        0x04,
                        0x08, 0x4B, 0x65, 0x79, 0x33,
                        0x02,
                        0x08, 0x4B, 0x65, 0x79, 0x34,
                        0x00,

                        0x3D,
                        0x0A,

                        0x08, 0x4B, 0x65, 0x79, 0x35,
                        0x00,
                        0x08, 0x4B, 0x65, 0x79, 0x36,
                        0x02,
                        0x08, 0x4B, 0x65, 0x79, 0x37,
                        0x04,
                        0x08, 0x4B, 0x65, 0x79, 0x38,
                        0x06,
                        0x08, 0x4B, 0x65, 0x79, 0x39,
                        0x08,

                        0x00
                    }
                };
            }
        }
    }
}
