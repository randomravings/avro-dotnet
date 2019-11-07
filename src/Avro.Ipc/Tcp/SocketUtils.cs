using Avro.Ipc.IO;
using Avro.Ipc.Utils;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Tcp
{
    internal static class SocketUtils
    {
        public static async Task<int> SendAsync(NetworkStream stream, FrameStream frames, CancellationToken token)
        {
            var bytesSent = 0;
            foreach (var frame in frames.GetBuffers())
            {
                var frameBytes = new byte[4];
                MessageFramingUtil.EncodeLength((int)frame.Length, frameBytes, 0);
                await stream.WriteAsync(frameBytes, 0, 4, token);
                bytesSent += 4;
                await stream.WriteAsync(frame.GetBuffer(), 0, (int)frame.Length, token);
                bytesSent += (int)frame.Length;
            }
            return bytesSent;
        }

        public static async Task<FrameStream> ReceiveAsync(NetworkStream stream, CancellationToken token)
        {
            var result = new FrameStream();
            var frameSizeBytes = new byte[4];
            await ReadBytesAsync(stream, frameSizeBytes, 0, 4, token);
            var frameSize = MessageFramingUtil.DecodeLength(frameSizeBytes, 0);
            if (frameSize == 0)
                return FrameStream.EMPTY;

            var frame = new MemoryStream(new byte[frameSize], 0, frameSize, true, true);
            await ReadBytesAsync(stream, frame.GetBuffer(), 0, frameSize, token);
            frame.SetLength(frameSize);
            result.AppendFrame(frame);
            return result;
        }

        private static async Task ReadBytesAsync(NetworkStream stream, byte[] buffer, int offset, int count, CancellationToken token)
        {
            var bytesRead = offset;
            while (bytesRead < count)
                bytesRead += await stream.ReadAsync(buffer, bytesRead, count - bytesRead, token);
        }
    }
}
