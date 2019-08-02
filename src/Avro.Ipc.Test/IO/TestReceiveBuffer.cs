using Avro.Ipc.Utils;
using NUnit.Framework;
using System.IO;

namespace Avro.Ipc.IO.Test
{
    public class Tests
    {
        [Test]
        public void TestReceiveBuffer()
        {
            var receiveBuffer = new ReadBuffer();

            var buffer0 = new byte[14];
            var buffer1 = new byte[14];
            var buffer2 = new byte[14];

            for (byte i = 0; i < 10; i++)
            {
                buffer0[i + 4] = i;
                buffer1[i + 4] = (byte)(40 + i);
                buffer2[i + 4] = (byte)(20 + i);
            }

            MessageFramingUtil.EncodeLength(10, buffer0, 0);
            MessageFramingUtil.EncodeLength(10, buffer1, 0);
            MessageFramingUtil.EncodeLength(10, buffer2, 0);

            receiveBuffer.AddSegment(buffer0);
            receiveBuffer.AddSegment(buffer1);
            receiveBuffer.AddSegment(buffer2);

            Assert.AreEqual(30, receiveBuffer.Length);

            var expectedResult = new byte[]
            {
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
                20, 21, 22, 23, 24, 25, 26, 27, 28, 29
            };

            var bytes = new byte[30];

            receiveBuffer.Seek(0, SeekOrigin.Begin);
            receiveBuffer.Read(bytes, 0, 30);

            Assert.AreEqual(bytes, expectedResult);
        }

    }
}