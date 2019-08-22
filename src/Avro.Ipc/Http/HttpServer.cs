using System.Collections.Generic;
using System.Net;

namespace Avro.Ipc.Http
{
    public static class HttpServer
    {
        public static ITranceiver Create(IEnumerable<string> uriPrefixes)
        {
            var httpListener = new HttpListener();
            foreach (var uriPrefix in uriPrefixes)
                httpListener.Prefixes.Add(uriPrefix);
            return new HttpTranceiver(httpListener);
        }

    }
}
