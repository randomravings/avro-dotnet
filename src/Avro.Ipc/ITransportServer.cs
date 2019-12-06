using Avro.Ipc.IO;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc
{
    public interface ITransportServer : IDisposable
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
        /// 
        /// </summary>
        /// <returns></returns>
        ITransportContext Receive();

        /// <summary>
        /// Reads next request from underlying stream.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<ITransportContext> ReceiveAsync(CancellationToken token);

        /// <summary>
        /// Closes the connection.
        /// </summary>
        void Close();
    }
}
