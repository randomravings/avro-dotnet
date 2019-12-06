using Avro.Ipc.IO;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc
{
    public interface ITransportClient : IDisposable
    {
        /// <summary>
        /// Flag indicating if the transport uses stateful connection.
        /// </summary>
        bool Stateful { get; }

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
        /// <returns></returns>
        FrameStream Request(string messageName, FrameStream frames);

        /// <summary>
        /// Request/Response call.
        /// </summary>
        /// <param name="messageName"></param>
        /// <param name="frames"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<FrameStream> RequestAsync(string messageName, FrameStream frames, CancellationToken token);

        /// <summary>
        /// One way call.
        /// </summary>
        /// <param name="messageName"></param>
        /// <param name="frames"></param>
        /// <returns></returns>
        void RequestOneWay(string messageName, FrameStream frames);

        /// <summary>
        /// One way call.
        /// </summary>
        /// <param name="messageName"></param>
        /// <param name="frames"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task RequestOneWayAsync(string messageName, FrameStream frames, CancellationToken token);

        /// <summary>
        /// Closes the connection.
        /// </summary>
        void Close();
    }
}
