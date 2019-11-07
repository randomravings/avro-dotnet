using Avro.Protocol;
using Avro.Types;
using org.apache.avro.ipc;
using System.Collections.Generic;

namespace Avro.Ipc
{
    public class GenericContext
    {
        public IDictionary<string, byte[]> Metadata { get; set; } = new Dictionary<string, byte[]>();
        public string MessageName { get; set; } = string.Empty;
        public GenericRecord RequestParameters { get; set; } = GenericRecord.Empty;
        public bool IsError { get; set; } = false;
        public GenericResponse Response { get; set; } = new GenericResponse();
        public GenericResponseError Error { get; set; } = new GenericResponseError();
        internal HandshakeRequest HandshakeRequest { get; set; } = new HandshakeRequest();
        internal HandshakeResponse HandshakeResponse { get; set; } = new HandshakeResponse();
    }
}
