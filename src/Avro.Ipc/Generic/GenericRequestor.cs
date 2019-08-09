using Avro.Generic;
using Avro.IO;
using System;
using System.Collections.Generic;
using System.Text;

namespace Avro.Ipc.Generic
{
    public class GenericRequestor
    {
        private readonly GenericProtocol _protocol;

        public GenericRequestor(GenericProtocol protocol)
        {
            _protocol = protocol;
        }

        public void WriteRequest(GenericMessage message, IEncoder encoder)
        {

        }

        public GenericRecord ReadResponse(IDecoder decoder)
        {
            return null;
        }

        public object ReadError(IDecoder decoder)
        {
            return null;
        }
    }
}
