using Avro.Ipc.IO;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc
{
    public interface IServer : IDisposable
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
        /// Sends a response to client.
        /// </summary>
        /// <param name="frames"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<int> SendAsync(FrameStream frames, CancellationToken token = default);

        /// <summary>
        /// Reads next request from underlying stream.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<FrameStream> ReceiveAsync(CancellationToken token = default);

        /// <summary>
        /// Closes the connection.
        /// </summary>
        void Close();
    }
}
