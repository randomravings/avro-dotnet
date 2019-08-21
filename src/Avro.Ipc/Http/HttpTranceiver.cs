using Avro.Ipc.IO;
using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Http
{
    public class HttpTranceiver : ITranceiver
    {
        private readonly HttpListenerContext _context;
        public HttpTranceiver(string url)
        {
            RemoteEndPoint = url;
        }
        public HttpTranceiver(HttpListenerContext context)
        {
            RemoteEndPoint = _context.Request.Url.ToString();
            _context = context;
        }

        public string LocalEndPoint => IPAddress.Loopback.ToString();

        public string RemoteEndPoint { get; private set; }

        public void Close() { }

        public async Task<int> SendAsync(FrameStream frames, CancellationToken token)
        {
            await frames.CopyToAsync(_context.Response.OutputStream);
            _context.Response.OutputStream.Flush();
            return (int)_context.Response.OutputStream.Length;
        }

        public async Task<FrameStream> ReceiveAsync(CancellationToken token)
        {
            var frames = new FrameStream();
            await _context.Request.InputStream.CopyToAsync(frames);
            return frames;
        }

        public async Task<FrameStream> RequestAsync(FrameStream frames, CancellationToken token)
        {
            var request = WebRequest.CreateHttp(RemoteEndPoint);
            request.Method = "POST";
            request.ContentType = "avro/binary";

            var requestStream = await request.GetRequestStreamAsync();
            frames.Seek(0, SeekOrigin.Begin);
            await frames.CopyToAsync(requestStream);

            var response = await request.GetResponseAsync();
            return null;
        }

        public bool TestConnection()
        {
            throw new NotImplementedException();
        }
    }
}
