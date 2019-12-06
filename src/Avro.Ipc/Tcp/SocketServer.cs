using Avro.Ipc.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Tcp
{
    public sealed class SocketServer : ITransportServer
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

        public bool Stateful => true;

        public string LocalEndPoint => _client.Client.LocalEndPoint.ToString();
        public string RemoteEndPoint => _client.Client.RemoteEndPoint.ToString();

        public async Task<int> SendAsync(FrameStream frames, CancellationToken token) => await SocketUtils.SendAsync(_client.GetStream(), frames, token);

        public ITransportContext Receive()
        {
            var requetsData = SocketUtils.Receive(_client.GetStream());
            return new SocketContext(
                requetsData,
                (b) => SocketUtils.Send(_client.GetStream(), requetsData),
                (b, c) => SocketUtils.SendAsync(_client.GetStream(), requetsData, c)
            );
        }

        public async Task<ITransportContext> ReceiveAsync(CancellationToken token)
        {
            var requetsData = await SocketUtils.ReceiveAsync(_client.GetStream(), token);
            return new SocketContext(
                requetsData,
                (b) => SocketUtils.Send(_client.GetStream(), requetsData),
                (b, c) => SocketUtils.SendAsync(_client.GetStream(), requetsData, c)
            );
        }

        public void Close() => _client.Close();

        public void Dispose() => _client.Dispose();
    }
}
