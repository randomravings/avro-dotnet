using Avro.Ipc.IO;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.Local
{
    public class LocalContext : ITransportContext
    {
        private readonly Func<FrameStream, int> _responder;
        private readonly Func<FrameStream, CancellationToken, Task<int>> _responderAsync;

        public LocalContext(FrameStream requestData, Func<FrameStream, int> responder, Func<FrameStream, CancellationToken, Task<int>> responderAsync)
        {
            _responder = responder;
            _responderAsync = responderAsync;
            RequestData = requestData;
        }

        public FrameStream RequestData { get; private set; }

        public int Respond(FrameStream responseData) => _responder.Invoke(responseData);

        public Task<int> RespondAsync(FrameStream responseData, CancellationToken token) => _responderAsync.Invoke(responseData, token);

        public void Dispose() { }
    }
}
