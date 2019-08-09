using System;
using System.Collections.Generic;
using System.Text;
using Avro.IO;
using Avro.Protocols;

namespace Avro.Generic
{
    public class GenericMessage
    {
        private readonly IReadOnlyDictionary<string, GenericRecord> _namedParameter;
        private Message messageType;

        public GenericMessage(Message messageType)
        {
            this.messageType = messageType;
        }

        public string Name { get; internal set; }

        internal void SerializeRequest(BinaryEncoder encoder)
        {
            
        }

        internal void DeserializeResponse(BinaryDecoder encoder)
        {
            
        }
    }
}
