using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Avro.Ipc.Tcp
{
    public sealed class SocketListener
    {
        private readonly TcpListener _server;
        private readonly IPAddress _addr;
        private readonly int _port;
        public SocketListener(string ip, int port)
        {
            _addr = IPAddress.Parse(ip);
            _port = port;
            _server = new TcpListener(_addr, _port);
        }
        public void Start() => _server.Start();
        public void Stop() => _server.Stop();
        public async Task<SocketServer> ListenAsync()
        {
            var tcpClient = await _server.AcceptTcpClientAsync();
            return new SocketServer(tcpClient);
        }
    }
}
