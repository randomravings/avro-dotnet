using Avro.Ipc.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc
{
    public interface ITranceiver
    {
        string LocalEndPoint { get; }
        string RemoteEndPoint { get; }
        bool TestConnection();
        Task<int> SendAsync(FrameStream frames, CancellationToken token);
        Task<FrameStream> ReceiveAsync(CancellationToken token);
        Task<FrameStream> RequestAsync(string messageName, FrameStream frames, CancellationToken token);
        void Close();
    }
}