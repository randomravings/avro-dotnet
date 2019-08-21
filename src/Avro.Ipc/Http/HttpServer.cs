using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Avro.Ipc.Http
{
    public class HttpServer
    {
        private HttpListener _httpListener;

        public HttpServer(params string[] uriPrefixes)
        {
            _httpListener = new HttpListener();
            foreach (var uriPrefix in uriPrefixes)
                _httpListener.Prefixes.Add(uriPrefix);
        }

        public void Start() => _httpListener.Start();
        public void Stop() => _httpListener.Stop();

        public async Task<HttpListenerContext> ListenAsync()
        {
            return await _httpListener.GetContextAsync();
        }

    }
}
