using Avro.Ipc.IO;
using Avro.Ipc.Utils;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Tcp
{
    public sealed class SocketTranceiver : ITranceiver
    {
        private readonly TcpClient _client;
        private readonly NetworkStream _stream;

        public SocketTranceiver(TcpClient client)
        {
            _client = client;
            _client.NoDelay = true;
            _stream = _client.GetStream();
        }

        public bool TestConnection()
        {
            return _client.Connected;
        }

        public string LocalEndPoint => _client.Client.LocalEndPoint.ToString();
        public string RemoteEndPoint => _client.Client.RemoteEndPoint.ToString();

        public async Task<int> SendAsync(FrameStream frames, CancellationToken token)
        {
            var bytesSent = 0;
            foreach (var frame in frames.GetBuffers())
            {
                var frameBytes = new byte[4];
                MessageFramingUtil.EncodeLength((int)frame.Length, frameBytes, 0);
                await _stream.WriteAsync(frameBytes, 0, 4, token);
                bytesSent += 4;
                await _stream.WriteAsync(frame.GetBuffer(), 0, (int)frame.Length, token);
                bytesSent += (int)frame.Length;
            }
            return bytesSent;
        }

        public async Task<FrameStream> ReceiveAsync(CancellationToken token)
        {
            var result = new FrameStream();
            var frameSizeBytes = new byte[4];
            await ReadBytesAsync(_stream, frameSizeBytes, 0, 4, token);
            var frameSize = MessageFramingUtil.DecodeLength(frameSizeBytes, 0);
            if (frameSize == 0)
                return FrameStream.EMPTY;

            var frame = new MemoryStream(new byte[frameSize], 0, frameSize, true, true);
            await ReadBytesAsync(_stream, frame.GetBuffer(), 0, frameSize, token);
            frame.SetLength(frameSize);
            result.AppendFrame(frame);
            result.Seek(0, SeekOrigin.Begin);
            return result;
        }

        public async Task<FrameStream> RequestAsync(FrameStream frames, CancellationToken token)
        {
            await SendAsync(frames, token);
            return await ReceiveAsync(token);
        }

        private static async Task ReadBytesAsync(NetworkStream stream, byte[] buffer, int offset, int count, CancellationToken token)
        {
            var bytesRead = offset;
            while (bytesRead < count)
                bytesRead += await stream.ReadAsync(buffer, bytesRead, count - bytesRead, token);
        }

        public void Close()
        {
            _client.Close();
        }
    }
}
