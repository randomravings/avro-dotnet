using Avro.Protocol.Schema;
using Avro.Schema;
using Avro.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Avro
{
    public class AvroProtocol : AvroMeta, IEquatable<AvroProtocol>
    {
        private static readonly MD5 HASH = System.Security.Cryptography.MD5.Create();
        private string _name;
        private string _namespace;
        private readonly List<NamedSchema> _types = new List<NamedSchema>();
        private readonly List<MessageSchema> _messages = new List<MessageSchema>();

        public AvroProtocol()
            : this(null, null) { }

        public AvroProtocol(string name)
            : this(name, null) { }

        public AvroProtocol(string name, string ns)
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
        }

        public string Name { get { return _name; } set { NameValidator.ValidateName(value); _name = value; } }
        public string Namespace
        {
            get
            {
                return _namespace;
            }
            set
            {
                if (_namespace == value)
                    return;
                NameValidator.ValidateNamespace(value);
                var old = _namespace;
                _namespace = value;

                var namedTypes =
                    Types.Where(r => r is NamedSchema)
                        .Select(t => t as NamedSchema)
                        .Where(r => r.Namespace == old || r.Namespace != null && r.Namespace.StartsWith(old))
                    ;

                foreach (var namedType in namedTypes)
                    if (string.IsNullOrEmpty(namedType.Namespace) || namedType.Namespace == old)
                        namedType.Namespace = value;
                    else
                        namedType.Namespace = $"{value}{namedType.Namespace.Remove(0, old.Length)}";
            }
        }
        public string FullName => string.IsNullOrEmpty(Namespace) ? Name : $"{Namespace}.{Name}";
        public string Doc { get; set; }
        public byte[] MD5 => HASH.ComputeHash(Encoding.UTF8.GetBytes(this.ToAvroCanonical()));

        public IReadOnlyList<NamedSchema> Types => _types.AsReadOnly();
        public IReadOnlyList<MessageSchema> Messages => _messages.AsReadOnly();

        public void AddType(NamedSchema schema)
        {
            if (string.IsNullOrEmpty(schema.Namespace))
                schema.Namespace = Namespace;
            if (_types.Contains(schema))
                throw new AvroException($"Protocol already contains the type: '{schema.FullName}'");
            _types.Add(schema);
        }

        public void AddMessage(MessageSchema message)
        {
            if (_messages.Contains(message))
                throw new AvroException($"Protocol already contains the message: '{message.Name}'");
            foreach (var request in message.RequestParameters)
                if (_types.FirstOrDefault(r => r.FullName == request.Type.FullName) == null)
                    throw new AvroException($"Protocol does not contain type: '{request.Type}'");
            _messages.Add(message);
        }

        public bool Equals(AvroProtocol other)
        {
            return other.FullName == FullName;
        }

        public override string ToString() => FullName;

        public static AvroProtocol Parse(string text) => AvroParser.ReadProtocol(text);
    }
}
