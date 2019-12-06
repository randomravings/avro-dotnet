using Avro.IO;
using Avro.Ipc.IO;
using Avro.Protocol;
using Avro.Types;
using org.apache.avro.ipc;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc
{
    public class GenericContext : IDisposable
    {
        private readonly FrameStream _responseStream;
        private readonly ITransportContext _context;

        public GenericContext(ITransportContext transportContext)
        {
            _context = transportContext;
            _context.RequestData.Seek(0, SeekOrigin.Begin);
            _responseStream = new FrameStream();
            RequestDecoder = new BinaryDecoder(transportContext.RequestData);
            ResponseEncoder = new BinaryEncoder(_responseStream);
        }
        public IAvroDecoder RequestDecoder { get; private set; }
        public HandshakeRequest? HandshakeRequest { get; set; }
        public IDictionary<string, byte[]> RequestMetadata { get; set; } = new Dictionary<string, byte[]>();
        public string MessageName { get; set; } = string.Empty;
        public GenericRecord Parameters { get; set; } = GenericRecord.Empty;
        public IAvroEncoder ResponseEncoder { get; private set; }
        public HandshakeResponse? HandshakeResponse { get; set; }
        public IDictionary<string, byte[]> ResponseMetadata { get; set; } = new Dictionary<string, byte[]>();
        public bool IsError { get; set; }
        public GenericResponse Response { get; set; } = GenericResponse.Empty;
        public GenericResponseError Error { get; set; } = GenericResponseError.Empty;
        public int Dispatch() => _context.Respond(_responseStream);
        public async Task<int> DispatchAsync(CancellationToken token) => await _context.RespondAsync(_responseStream, token);
        public void Dispose()
        {
            RequestDecoder.Dispose();
            ResponseEncoder.Dispose();
            _context.Dispose();
            _responseStream.Dispose();
        }
    }
}
