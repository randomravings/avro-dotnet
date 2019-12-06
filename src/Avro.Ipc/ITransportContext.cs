using Avro.Ipc.IO;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc
{
    public interface ITransportContext : IDisposable
    {
        public FrameStream RequestData { get; }
        public int Respond(FrameStream responseData);
        public Task<int> RespondAsync(FrameStream responseData, CancellationToken token);
    }
}
