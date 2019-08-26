using Avro.Generic;
using Avro.Types;
using org.apache.avro.ipc;
using System;
using System.Collections.Generic;

namespace Avro.Ipc.Generic
{
    public class GenericContext
    {
        public IDictionary<string, byte[]> Metadata { get; set; }
        public string MessageName { get; set; }
        public GenericAvroRecord RequestParameters { get; set; }
        public bool IsError { get; set; }
        public object Response { get; set; }
        public object Error { get; set; }
        internal HandshakeRequest HandshakeRequest { get; set; }
        internal HandshakeResponse HandshakeResponse { get; set; }
    }
}
