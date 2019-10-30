using System;
using System.Threading.Tasks;

namespace Avro.Ipc.Http
{
    public static class HttpClient
    {
        public static async Task<ITranceiver> ConnectAsync(string url)
        {
            return await Task<ITranceiver>.Factory.StartNew(() => {
                var localUrl = url.EndsWith('/') ? url : $"{url}/";
                return new HttpTranceiver(new Uri(localUrl));
            }).ConfigureAwait(false);
        }
    }
}
