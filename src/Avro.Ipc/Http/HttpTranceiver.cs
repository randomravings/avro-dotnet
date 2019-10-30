using Avro.Ipc.IO;
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Http
{
    public class HttpTranceiver : ITranceiver
    {
        private readonly HttpListener _httpListener;
        private HttpListenerContext? _context;
        private readonly Uri _remoteUri;
        public HttpTranceiver(Uri remoteUrl)
        {
            _remoteUri = remoteUrl;
        }
        public HttpTranceiver(HttpListener httpListener)
        {
            _httpListener = httpListener;
            _httpListener.Start();
        }

        public string LocalEndPoint => IPAddress.Loopback.ToString();

        public string RemoteEndPoint => _remoteUri.AbsoluteUri;

        public void Close()
        {
            _httpListener?.Close();
        }

        public async Task<int> SendAsync(FrameStream frames, CancellationToken token)
        {
            var len = (int)(frames.Length - frames.Position);
            await frames.CopyToAsync(_context.Response.OutputStream);
            _context.Response.OutputStream.Flush();
            _context.Response.Close();
            return len;
        }

        public async Task<FrameStream> ReceiveAsync(CancellationToken token)
        {
            var frames = new FrameStream();
            _context = await _httpListener.GetContextAsync();
            await _context.Request.InputStream.CopyToAsync(frames);
            return frames;
        }

        public async Task<FrameStream> RequestAsync(string messageName, FrameStream frames, CancellationToken token)
        {
            var request = WebRequest.CreateHttp(new Uri(_remoteUri, messageName));
            request.Method = "POST";
            request.ContentType = "avro/binary";

            var requestStream = await request.GetRequestStreamAsync();
            await frames.CopyToAsync(requestStream);

            var response = await request.GetResponseAsync();

            var result = new FrameStream();
            using (var resonseStream = response.GetResponseStream())
                await resonseStream.CopyToAsync(result, token);
            return result;
        }

        public bool TestConnection()
        {
            throw new NotImplementedException();
        }
    }
}
