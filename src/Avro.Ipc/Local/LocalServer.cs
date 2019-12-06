using Avro.Ipc.IO;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Local
{
    public sealed class LocalServer : ITransportServer
    {
        private readonly BlockingCollection<FrameStream> _server;
        private readonly BlockingCollection<FrameStream> _client;

        public LocalServer(BlockingCollection<FrameStream> server, BlockingCollection<FrameStream> client)
        {
            _server = server;
            _client = client;
        }

        public bool Stateful => true;

        public string LocalEndPoint => string.Empty;

        public string RemoteEndPoint => string.Empty;

        public void Close() { }

        public ITransportContext Receive()
        {
            var requestData = _client.Take();
            return new LocalContext(
                requestData,
                (b) => { _server.Add(b); return (int)b.Length; },
                (b, c) => Task<int>.Factory.StartNew(() => { _server.Add(b); return (int)b.Length; }, c)
            );
        }

        public async Task<ITransportContext> ReceiveAsync(CancellationToken token)
        {
            var requestData = await Task<FrameStream>.Factory.StartNew(() =>
            {
                return _client.Take(token);
            }, token).ConfigureAwait(false);
            return new LocalContext(
                requestData,
                (b) => { _server.Add(b); return (int)b.Length; },
                (b, c) => Task<int>.Factory.StartNew(() => { _server.Add(b); return (int)b.Length; }, c)
            );
        }

        public void Dispose() { }
    }
}
