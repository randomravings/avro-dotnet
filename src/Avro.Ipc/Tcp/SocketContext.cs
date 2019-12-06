using Avro.Ipc.IO;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Tcp
{
    public class SocketContext : ITransportContext
    {
        private readonly Func<FrameStream, int> _responder;
        private readonly Func<FrameStream, CancellationToken, Task<int>> _responderAsync;

        public SocketContext(FrameStream requestData, Func<FrameStream, int> responder, Func<FrameStream, CancellationToken, Task<int>> responderAsync)
        {
            _responder = responder;
            _responderAsync = responderAsync;
            RequestData = requestData;
        }

        public FrameStream RequestData { get; private set; }

        public int Respond(FrameStream reply) => _responder.Invoke(reply);

        public async Task<int> RespondAsync(FrameStream reply, CancellationToken token) => await _responderAsync.Invoke(reply, token);

        public void Dispose() => RequestData.Dispose();
    }
}
