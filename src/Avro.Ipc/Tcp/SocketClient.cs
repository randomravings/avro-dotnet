using System.Net.Sockets;
using System.Threading.Tasks;

namespace Avro.Ipc.Tcp
{
    public static class SocketClient
    {
        public static async Task<ITranceiver> ConnectAsync(string host, int port)
        {
            var tcpClient = new TcpClient();
            await tcpClient.ConnectAsync(host, port);
            return new SocketTranceiver(tcpClient);
        }
    }
}
