using Avro.Protocols;
using Avro.Schemas;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Avro
{
    public class Protocol
    {
        private readonly IList<NamedSchema> _types;
        private readonly IList<Message> _messages;

        /// <summary>
        /// Constructor for Protocol class
        /// </summary>
        public Protocol()
        {
            _types = new List<NamedSchema>();
            _messages = new List<Message>();
        }

        /// <summary>
        /// Constructor for Protocol class
        /// </summary>
        public Protocol(string name)
        {
            Name = name;
            _types = new List<NamedSchema>();
            _messages = new List<Message>();
        }

        public byte[] MD5 => System.Security.Cryptography.MD5.Create().ComputeHash(Encoding.UTF8.GetBytes(ToString()));

        public string FullName => string.IsNullOrEmpty(Namespace) ? Name : $"{Namespace}.{Name}";

        /// <summary>
        /// Name of the protocol
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Namespace of the protocol
        /// </summary>
        public string Namespace { get; set; }

        /// <summary>
        /// Documentation for the protocol
        /// </summary>
        public string Doc { get; set; }

        /// <summary>
        /// List of schemas objects representing the different schemas defined under the 'types' attribute
        /// </summary>
        public IList<NamedSchema> Types { get; set; }

        /// <summary>
        /// List of message objects representing the different schemas defined under the 'messages' attribute
        /// </summary>
        public IList<Message> Messages { get; set; }

        public override string ToString() => FullName;

        public static Protocol Parse(string text)
        {
            var protocol = ProtocolParser.Parse(text);
            return protocol;
        }
    }
}
