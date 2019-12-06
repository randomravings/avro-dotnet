using Avro.Ipc.IO;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Http
{
    public class HttpServer : ITransportServer
    {
        private readonly HttpListener _httpListener;

        public HttpServer(IEnumerable<string> uriPrefixes)
        {
            _httpListener = new HttpListener();
            foreach (var uriPrefix in uriPrefixes)
                _httpListener.Prefixes.Add(uriPrefix);
            _httpListener.Start();
        }

        public bool Stateful => false;

        public string LocalEndPoint => string.Join(";", _httpListener.Prefixes);

        public string RemoteEndPoint => "";

        public ITransportContext Receive()
        {
            var requestData = new FrameStream();
            var context = _httpListener.GetContext();
            context.Request.InputStream.CopyTo(requestData);
            return new HttpContext(requestData, context);
        }

        public async Task<ITransportContext> ReceiveAsync(CancellationToken token)
        {
            var requestData = new FrameStream();
            var context = await _httpListener.GetContextAsync();
            await context.Request.InputStream.CopyToAsync(requestData);
            return new HttpContext(requestData, context);
        }

        public void Close()
        {
            _httpListener.Stop();
            _httpListener.Close();
        }

        public void Dispose() => _httpListener.Close();
    }
}
