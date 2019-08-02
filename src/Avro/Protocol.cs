using Avro.Protocols;
using Avro.Schemas;
using Avro.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Avro
{
    public class Protocol : AvroObject, IEquatable<Protocol>
    {
        private string _name;
        private string _nameSpace;
        private readonly List<NamedSchema> _types;
        private readonly List<Message> _messages;

        public Protocol()
            : this(null, null) { }

        public Protocol(string name)
            : this(name, null) { }

        public Protocol(string name, string ns)
            : base()
        {
            var items = name?.Split('.') ?? new string[0];
            if (items.Length > 1 && ns == null)
            {
                Name = items.Last();
                Namespace = string.Join(".", items.Take(items.Length - 1));
            }
            else
            {
                if (name != null)
                    Name = name;
                Namespace = ns;
            }
            _types = new List<NamedSchema>();
            _messages = new List<Message>();
        }

        public string Name { get { return _name; } set { NameValidator.ValidateName(value); _name = value; } }
        public string Namespace { get { return _nameSpace; } set { NameValidator.ValidateNamespace(value); _nameSpace = value; } }
        public string FullName => string.IsNullOrEmpty(Namespace) ? Name : $"{Namespace}.{Name}";
        public string Doc { get; set; }
        public byte[] MD5 => System.Security.Cryptography.MD5.Create().ComputeHash(Encoding.UTF8.GetBytes(this.ToAvroCanonical()));

        public IReadOnlyList<NamedSchema> Types => _types.AsReadOnly();
        public IReadOnlyList<Message> Messages => _messages.AsReadOnly();

        public void AddType(NamedSchema schema)
        {
            if (_types.Contains(schema))
                throw new AvroException($"Protocol already contains the type: '{schema.FullName}'");
            _types.Add(schema);
        }

        public void AddMessage(Message message)
        {
            if (_messages.Contains(message))
                throw new AvroException($"Protocol already contains the message: '{message.Name}'");
            foreach (var request in message.RequestParameters)
                if (!_types.Contains(request.Type))
                    throw new AvroException($"Protocol does not contain type: '{request.Type.FullName}'");
            _messages.Add(message);
        }

        public bool Equals(Protocol other)
        {
            return other.FullName == FullName;
        }

        public override string ToString() => FullName;

        public static Protocol Parse(string text) => AvroReader.ReadProtocol(text);
    }
}
