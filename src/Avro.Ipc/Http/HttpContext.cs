using Avro.Ipc.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Http
{
    public class HttpContext : ITransportContext
    {
        private readonly HttpListenerContext _context;

        public HttpContext(FrameStream requestData, HttpListenerContext context)
        {
            _context = context;
            RequestData = requestData;
        }

        public FrameStream RequestData { get; private set; }

        public int Respond(FrameStream responseData)
        {
            var pos = responseData.Position;
            responseData.CopyTo(_context.Response.OutputStream);
            _context.Response.OutputStream.Flush();
            _context.Response.Close();
            return (int)(responseData.Position - pos);
        }

        public async Task<int> RespondAsync(FrameStream responseData, CancellationToken token)
        {
            var pos = responseData.Position;
            await responseData.CopyToAsync(_context.Response.OutputStream);
            await _context.Response.OutputStream.FlushAsync();
            _context.Response.Close();
            return (int)(responseData.Position - pos);
        }

        public void Dispose() { }
    }
}
