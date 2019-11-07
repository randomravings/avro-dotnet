using Avro.Ipc.IO;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc
{
    public interface IClient : IDisposable
    {
        /// <summary>
        /// Local end point address.
        /// </summary>
        string LocalEndPoint { get; }

        /// <summary>
        /// Remote end point address. For stateless connections this value is empty.
        /// </summary>
        string RemoteEndPoint { get; }

        /// <summary>
        /// Tests the connection.
        /// </summary>
        /// <returns></returns>
        bool TestConnection();

        /// <summary>
        /// Request/Response call.
        /// </summary>
        /// <param name="messageName"></param>
        /// <param name="frames"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<FrameStream> RequestAsync(string messageName, FrameStream frames, CancellationToken token = default);

        /// <summary>
        /// One way call.
        /// </summary>
        /// <param name="messageName"></param>
        /// <param name="frames"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task RequestOneWayAsync(string messageName, FrameStream frames, CancellationToken token = default);

        /// <summary>
        /// Closes the connection.
        /// </summary>
        void Close();
    }
}
