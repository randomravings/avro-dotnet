using Avro.Ipc.IO;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Local
{
    public sealed class LocalClient : IClient
    {
        private readonly BlockingCollection<FrameStream> _client;
        private readonly BlockingCollection<FrameStream> _server;

        public LocalClient(BlockingCollection<FrameStream> client, BlockingCollection<FrameStream> server)
        {
            _client = client;
            _server = server;
        }

        public string LocalEndPoint => string.Empty;

        public string RemoteEndPoint => string.Empty;

        public void Close() { }

        public async Task<FrameStream> ReceiveAsync(CancellationToken token)
        {
            return await Task<FrameStream>.Factory.StartNew(() =>
            {
                return _server.Take(token);
            }, token).ConfigureAwait(false);
        }

        public async Task<int> SendAsync(FrameStream frames, CancellationToken token)
        {
            return await Task<int>.Factory.StartNew(() =>
            {
                _client.Add(frames, token);
                return (int)frames.Length;
            }, token).ConfigureAwait(false); ;
        }

        public async Task<FrameStream> RequestAsync(string messageName, FrameStream frames, CancellationToken token)
        {
            return await Task<FrameStream>.Factory.StartNew(() =>
            {
                _server.Add(frames, token);
                return _client.Take(token);
            }, token).ConfigureAwait(false);
        }

        public async Task RequestOneWayAsync(string messageName, FrameStream frames, CancellationToken token = default)
        {
            await Task.Factory.StartNew(() =>
            {
                _server.Add(frames, token);
            }, token).ConfigureAwait(false);
        }

        public bool TestConnection() => true;

        public void Dispose() { }
    }
}
