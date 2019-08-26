using Avro.IO;
using Avro.Protocols;
using Avro.Types;
using System.Collections.Generic;

namespace Avro.Generic
{
    public class GenericMessage
    {
        private readonly IReadOnlyDictionary<string, GenericAvroRecord> _namedParameter;
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
