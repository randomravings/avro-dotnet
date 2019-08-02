using Avro.Ipc.IO;
using Avro.Ipc.Utils;
using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc
{
    public abstract class Session
    {
        private readonly object _guard = new object();
        private readonly TcpClient _tcpClient;
        private readonly int _readBufferSize;
        private CancellationTokenSource _cancellationTokenSource;
        private Task _task;

        public Session(string key, TcpClient tcpClient)
        {
            Key = key;
            _tcpClient = tcpClient;
            _readBufferSize = 1028;
        }
        public string Key { get; private set; }

        public void Start()
        {
            lock (_guard)
            {
                _cancellationTokenSource = new CancellationTokenSource();
                _task = new Task(() => Read(_cancellationTokenSource.Token));
                _task.Start();
            }
        }

        public void Stop()
        {
            lock (_guard)
                _cancellationTokenSource.Cancel();
        }

        private void Read(CancellationToken token)
        {
            var stream = _tcpClient.GetStream();
            var readBuffer = new ReadBuffer();
            while (true)
            {
                var segment = new byte[_readBufferSize];
                try
                {
                    var len = stream.ReadAsync(segment, 4, _readBufferSize, token).Result;
                    if (len >= 0)
                    {
                        MessageFramingUtil.EncodeLength(len, segment, 0);
                        readBuffer.AddSegment(segment);
                    }
                    else
                    {
                        // Invoke event
                        readBuffer = new ReadBuffer();
                    }
                }
                catch(OperationCanceledException)
                {
                    break;
                }
            }
        }

        protected abstract void HandleRequest(ReadBuffer readBuffer);
    }
}
