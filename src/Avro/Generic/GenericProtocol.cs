using Avro.IO;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;

namespace Avro.Generic
{
    public class GenericProtocol
    {
        private readonly Protocol _protocol;
        private readonly IReadOnlyList<GenericMessage> _messages;
        private readonly IReadOnlyDictionary<string, int> _requestLookup;

        public GenericProtocol(Protocol protocol)
        {
            _protocol = protocol;

            var messages = new List<GenericMessage>();
            foreach (var messageType in protocol.Messages)
                messages.Add(new GenericMessage(messageType));
            _messages = messages.AsReadOnly();

            var requestLookup = new SortedList<string, int>();
            for (int i = 0; i < _messages.Count; i++)
                requestLookup.Add(_messages[i].Name, i);
            _requestLookup = new ReadOnlyDictionary<string, int>(requestLookup);
        }

        public object SendRequest(GenericMessage message)
        {
            using (var stream = new MemoryStream())
            using (var encoder = new BinaryEncoder(stream))
            {
                message.SerializeRequest(encoder);
            }
            return null;
        }
    }
}
