using Avro.Ipc.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Tcp
{
    public sealed class SocketClient : IClient
    {
        private readonly TcpClient _client;

        private SocketClient(TcpClient client)
        {
            _client = client;
            _client.NoDelay = true;
        }

        public string LocalEndPoint => _client.Client.LocalEndPoint.ToString();
        public string RemoteEndPoint => _client.Client.RemoteEndPoint.ToString();

        public static async Task<SocketClient> ConnectAsync(string host, int port, CancellationToken token = default)
        {
            var tcpClient = new TcpClient();
            await tcpClient.ConnectAsync(host, port);
            return new SocketClient(tcpClient);
        }

        public void Close() => _client.Close();

        public void Dispose() => _client.Dispose();

        public async Task<FrameStream> RequestAsync(string messageName, FrameStream frames, CancellationToken token)
        {
            await SocketUtils.SendAsync(_client.GetStream(), frames, token);
            return await SocketUtils.ReceiveAsync(_client.GetStream(), token);
        }

        public async Task RequestOneWayAsync(string messageName, FrameStream frames, CancellationToken token = default)
        {
            await SocketUtils.SendAsync(_client.GetStream(), frames, token);
        }

        public bool TestConnection() => true;
    }
}
