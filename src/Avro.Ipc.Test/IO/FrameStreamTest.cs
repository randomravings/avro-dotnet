using Avro.Ipc.IO;
using NUnit.Framework;
using System.IO;

namespace Avro.Ipc.Test.IO
{
    [TestFixture]
    public class FrameBufferTest
    {
        [TestCase]
        public void TestReadWrite()
        {
            var buffer = new byte[1024 * 1024];
            for (int i = 0; i < buffer.Length; i++)
                buffer[i] = (byte)(i % byte.MaxValue);

            using (var stream = new FrameStream(512))
            using (var mem = new MemoryStream(new byte[1024 * 1024], 0, 1024 * 1024, true, true))
            {
                foreach (var b in buffer)
                    stream.WriteByte(b);

                stream.Seek(0, SeekOrigin.Begin);

                var read = new byte[1024 * 1024];
                stream.Read(read, 0, read.Length);
                Assert.AreEqual(buffer, read);

                stream.Seek(0, SeekOrigin.Begin);
                stream.CopyTo(mem);

                Assert.AreEqual(buffer, mem.GetBuffer());
            }
        }
    }
}
