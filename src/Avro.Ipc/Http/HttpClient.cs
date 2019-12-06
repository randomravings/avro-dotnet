using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Avro.Ipc.IO;

namespace Avro.Ipc.Http
{
    public class HttpClient : ITransportClient
    {
        private readonly Uri _remoteUri;

        public HttpClient(Uri remoteUrl)
        {
            _remoteUri = remoteUrl;
        }

        public bool Stateful => false;

        public string LocalEndPoint => IPAddress.Loopback.ToString();

        public string RemoteEndPoint => _remoteUri.AbsoluteUri;

        public void Connect()
        {
            throw new NotImplementedException();
        }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public void Dispose() { }

        public FrameStream Request(string messageName, FrameStream frames)
        {
            var request = WebRequest.CreateHttp(new Uri(_remoteUri, messageName));
            request.Method = "POST";
            request.ContentType = "avro/binary";

            var requestStream = request.GetRequestStream();
            frames.CopyTo(requestStream);

            var response = request.GetResponse();

            var result = new FrameStream();
            using var resonseStream = response.GetResponseStream();
            resonseStream.CopyTo(result);
            return result;
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

        public void RequestOneWay(string messageName, FrameStream frames)
        {
            throw new NotImplementedException();
        }

        public async Task RequestOneWayAsync(string messageName, FrameStream frames, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public bool TestConnection()
        {
            throw new NotImplementedException();
        }
    }
}
