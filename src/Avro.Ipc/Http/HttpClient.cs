using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Avro.Ipc.Http
{
    public static class HttpClient
    {
        public static async Task<ITranceiver> ConnectAsync(string url)
        {
            var localUrl = url.EndsWith('/') ? url : $"{url}/";
            return new HttpTranceiver(new Uri(localUrl));
        }
    }
}
