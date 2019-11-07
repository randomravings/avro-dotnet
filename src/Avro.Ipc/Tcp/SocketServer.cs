using Avro.Ipc.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Tcp
{
    public sealed class SocketServer : IServer
    {
        private readonly TcpClient _client;

        public SocketServer(TcpClient client)
        {
            _client = client;
            _client.NoDelay = true;
        }

        public bool TestConnection()
        {
            return _client.Connected;
        }

        public string LocalEndPoint => _client.Client.LocalEndPoint.ToString();
        public string RemoteEndPoint => _client.Client.RemoteEndPoint.ToString();

        public async Task<int> SendAsync(FrameStream frames, CancellationToken token) => await SocketUtils.SendAsync(_client.GetStream(), frames, token);

        public async Task<FrameStream> ReceiveAsync(CancellationToken token) => await SocketUtils.ReceiveAsync(_client.GetStream(), token);

        public void Close() => _client.Close();

        public void Dispose() => _client.Dispose();
    }
}
