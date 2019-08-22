using Avro.Ipc.IO;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Local
{
    public class LocalTranceiver : ITranceiver
    {
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
            }, token);
        }

        public async Task<int> SendAsync(FrameStream frames, CancellationToken token)
        {
            return await Task<int>.Factory.StartNew(() =>
            {
                _client.Add(frames, token);
                return (int)frames.Length;
            }, token);
        }
        
        public async Task<FrameStream> RequestAsync(string messageName, FrameStream frames, CancellationToken token)
        {
            return await Task<FrameStream>.Factory.StartNew(() =>
            {
                _server.Add(frames, token);
                return _client.Take(token);
            }, token);
        }

        public bool TestConnection()
        {
            return true;
        }
    }
}
