using Avro.Protocol;
using Avro.Types;
using org.apache.avro.ipc;
using System.Collections.Generic;

namespace Avro.Ipc
{
    public class GenericMessage
    {
        public IDictionary<string, byte[]> Metadata { get; set; } = new Dictionary<string, byte[]>();
        public string MessageName { get; set; } = string.Empty;
        public GenericRecord RequestParameters { get; set; } = GenericRecord.Empty;
        public bool IsError { get; set; } = false;
        public GenericResponse Response { get; set; } = GenericResponse.Empty;
        public GenericResponseError Error { get; set; } = GenericResponseError.Empty;
        internal HandshakeRequest HandshakeRequest { get; set; } = new HandshakeRequest();
        internal HandshakeResponse HandshakeResponse { get; set; } = new HandshakeResponse();
    }
}
