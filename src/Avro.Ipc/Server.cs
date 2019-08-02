using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Avro.Ipc
{
    public abstract class Server
    {
        private readonly object _guard = new object();
        private readonly IDictionary<string, Session> _clients;
        private TcpListener _server;
        private Task _task;

        public Server()
        {
            _clients = new Dictionary<string, Session>();
        }

        public void Start(int port)
        {
            Start("127.0.0.1", port);
        }

        public void Start(string ip, int port)
        {
            lock (_guard)
            {
                IPAddress addr = IPAddress.Parse(ip);
                _server = new TcpListener(addr, port);
                _server.Start();

                _task = new Task(() => Listen());
                _task.Start();
            }
        }

        public void Stop()
        {
            lock (_guard)
            {
                _server.Stop();
                _task.Wait();
            }
        }

        private void Listen()
        {
            while (true)
            {
                var tcpClient = _server.AcceptTcpClient();
                if (tcpClient == null)
                    break;
                lock (_guard)
                {
                    var key = tcpClient.Client.RemoteEndPoint.ToString();
                    if (_clients.TryGetValue(key, out var client))
                    {
                        client.Stop();
                        _clients.Remove(key);
                    }
                    var session = CreateSession(key, tcpClient);
                    _clients.Add(key, client);
                    client.Start();
                }
            }
        }

        protected abstract Session CreateSession(string key, TcpClient tcpClient);
    }
}
