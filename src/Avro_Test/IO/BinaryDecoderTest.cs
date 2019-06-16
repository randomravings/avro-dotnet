using Avro.IO;
using NUnit.Framework;
using System.IO;

namespace IO
{
    [TestFixture]
    public class BinaryDecoderTest
    {
        [SetUp]
        public void Setup()
        {
        }

        [TestCase(false, 1, new byte[] { 0x00 })]
        [TestCase(true, 1, new byte[] { 0x01 })]
        public void DecodeBoolean(bool expectedValue, int expectedLength, byte[] value)
        {
            using (var stream = new MemoryStream(value))
            using (var decoder = new BinaryDecoder(stream))
            {
                var decode = decoder.ReadBoolean();
                Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
                Assert.AreEqual(expectedValue, decode);

                stream.Seek(0, SeekOrigin.Begin);
                decoder.SkipBoolean();
                Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
            }
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
            using (var stream = new MemoryStream(value))
            using (var decoder = new BinaryDecoder(stream))
            {
                var decode = decoder.ReadInt();
                Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
                Assert.AreEqual(expectedValue, decode);

                stream.Seek(0, SeekOrigin.Begin);
                decoder.SkipInt();
                Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
            }
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
            using (var stream = new MemoryStream(value))
            using (var decoder = new BinaryDecoder(stream))
            {
                var decode = decoder.ReadLong();
                Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
                Assert.AreEqual(expectedValue, decode);

                stream.Seek(0, SeekOrigin.Begin);
                decoder.SkipLong();
                Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
            }
        }

        [TestCase(0, 4, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(-0.0012F, 4, new byte[] { 0x52, 0x49, 0x9D, 0xBA })]
        [TestCase(9.12e12F, 4, new byte[] { 0xA6, 0xB6, 0x04, 0x55 })]
        [TestCase(float.MinValue, 4, new byte[] { 0xFF, 0xFF, 0x7F, 0xFF })]
        [TestCase(float.MaxValue, 4, new byte[] { 0xFF, 0xFF, 0x7F, 0x7F })]
        public void DecodeFloat(float expectedValue, int expectedLength, byte[] value)
        {
            using (var stream = new MemoryStream(value))
            using (var decoder = new BinaryDecoder(stream))
            {
                var decode = decoder.ReadFloat();
                Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
                Assert.AreEqual(expectedValue, decode);

                stream.Seek(0, SeekOrigin.Begin);
                decoder.SkipFloat();
                Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
            }
        }

        [TestCase(0, 8, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(-89734587324924357.089734508973244D, 8, new byte[] { 0x1C, 0x18, 0x99, 0x1B, 0xD2, 0xEC, 0x73, 0xC3 })]
        [TestCase(9.12e12F, 8, new byte[] { 0x00, 0x00, 0x00, 0xC0, 0xD4, 0x96, 0xA0, 0x42 })]
        [TestCase(double.MinValue, 8, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEF, 0xFF })]
        [TestCase(double.MaxValue, 8, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEF, 0x7F })]
        public void DecodeDouble(double expectedValue, int expectedLength, byte[] value)
        {
            using (var stream = new MemoryStream(value))
            using (var decoder = new BinaryDecoder(stream))
            {
                var decode = decoder.ReadDouble();
                Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
                Assert.AreEqual(expectedValue, decode);

                stream.Seek(0, SeekOrigin.Begin);
                decoder.SkipDouble();
                Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
            }
        }

        [TestCase("Hello World!", 13, new byte[] { 0x18, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21 })]
        [TestCase("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 27, new byte[] { 0x34, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A })]
        [TestCase("zyxwvutsrqponmlkjihgfedcba", 27, new byte[] { 0x34, 0x7A, 0x79, 0x78, 0x77, 0x76, 0x75, 0x74, 0x73, 0x72, 0x71, 0x70, 0x6F, 0x6E, 0x6D, 0x6C, 0x6B, 0x6A, 0x69, 0x68, 0x67, 0x66, 0x65, 0x64, 0x63, 0x62, 0x61 })]
        [TestCase(" !#$%&'()*+,-./0123456789:;<=>?@", 33, new byte[] { 0x40, 0x20, 0x21, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F, 0x40 })]
        public void DecodeString(string expectedValue, int expectedLength, byte[] value)
        {
            using (var stream = new MemoryStream(value))
            using (var decoder = new BinaryDecoder(stream))
            {
                var decode = decoder.ReadString();
                Assert.AreEqual(expectedLength, stream.Position, "Decode offset error");
                Assert.AreEqual(expectedValue, decode);

                stream.Seek(0, SeekOrigin.Begin);
                decoder.SkipString();
                Assert.AreEqual(expectedLength, stream.Position, "Skip offset error");
            }
        }
    }
}
