using Avro.Ipc.IO;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Local
{
    public sealed class LocalServer : IServer
    {
        private readonly BlockingCollection<FrameStream> _server;
        private readonly BlockingCollection<FrameStream> _client;

        public LocalServer(BlockingCollection<FrameStream> server, BlockingCollection<FrameStream> client)
        {
            _server = server;
            _client = client;
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

        public void Dispose() { }
    }
}
