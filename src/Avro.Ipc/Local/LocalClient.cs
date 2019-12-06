using Avro.Ipc.IO;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Local
{
    public sealed class LocalClient : ITransportClient
    {
        private readonly BlockingCollection<FrameStream> _client;
        private readonly BlockingCollection<FrameStream> _server;

        public LocalClient(BlockingCollection<FrameStream> client, BlockingCollection<FrameStream> server)
        {
            _client = client;
            _server = server;
        }

        public bool Stateful => true;

        public string LocalEndPoint => string.Empty;

        public string RemoteEndPoint => string.Empty;

        public void Close() { }

        public FrameStream Request(string messageName, FrameStream frames)
        {
            _server.Add(frames);
            return _client.Take();
        }

        public async Task<FrameStream> RequestAsync(string messageName, FrameStream frames, CancellationToken token)
        {
            return await Task<FrameStream>.Factory.StartNew(() =>
            {
                _client.Add(frames, token);
                return _server.Take(token);
            }, token).ConfigureAwait(false);
        }

        public void RequestOneWay(string messageName, FrameStream frames)
        {
            _client.Add(frames);
        }

        public async Task RequestOneWayAsync(string messageName, FrameStream frames, CancellationToken token)
        {
            await Task.Factory.StartNew(() =>
            {
                _client.Add(frames, token);
            }, token).ConfigureAwait(false);
        }

        public bool TestConnection() => true;

        public void Dispose() { }
    }
}
