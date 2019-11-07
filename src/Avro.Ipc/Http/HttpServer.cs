using Avro.Ipc.IO;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Http
{
    public class HttpServer : IServer
    {
        private readonly HttpListener _httpListener;
        private HttpListenerContext? _context;

        public HttpServer(IEnumerable<string> uriPrefixes)
        {
            _httpListener = new HttpListener();
            foreach (var uriPrefix in uriPrefixes)
                _httpListener.Prefixes.Add(uriPrefix);
            _httpListener.Start();
        }

        public string LocalEndPoint => string.Join(";", _httpListener.Prefixes);

        public string RemoteEndPoint => "";

        public async Task<int> SendAsync(FrameStream frames, CancellationToken token = default)
        {
            var len = (int)(frames.Length - frames.Position);
            await frames.CopyToAsync(_context.Response.OutputStream);
            _context.Response.OutputStream.Flush();
            _context.Response.Close();
            return len;
        }

        public async Task<FrameStream> ReceiveAsync(CancellationToken token = default)
        {
            var frames = new FrameStream();
            _context = await _httpListener.GetContextAsync();
            await _context.Request.InputStream.CopyToAsync(frames);
            return frames;
        }

        public void Close()
        {
            _httpListener.Stop();
            _httpListener.Close();
        }

        public void Dispose() => _context = null;
    }
}
