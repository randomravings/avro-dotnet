using Avro.Ipc.IO;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Local
{
    public class LocalTranceiver : ITranceiver, IDisposable
    {
        private bool _disposed = false;
        private readonly BlockingCollection<FrameStream> _server;
        private readonly BlockingCollection<FrameStream> _client;

        public LocalTranceiver()
        {
            _server = new BlockingCollection<FrameStream>();
            _client = new BlockingCollection<FrameStream>();
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
            }, token).ConfigureAwait(false); ;
        }

        public bool TestConnection()
        {
            return true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;
            if (disposing)
            {
                _server.Dispose();
                _client.Dispose();
            }
            _disposed = true;
        }
    }
}
