using Avro.Schema;
using Avro.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Protocol.Schema
{
    public class MessageSchema : IEquatable<MessageSchema>
    {
        private readonly List<ParameterSchema> _requestParameters = new List<ParameterSchema>();
        private string _name = string.Empty;

        public MessageSchema(string name)
        {
            Name = name;
            Error = new UnionSchema(new StringSchema());
            _requestParameters = new List<ParameterSchema>();
        }

        public string Name { get { return _name; } set { NameValidator.ValidateName(value); _name = value; } }

        public string Doc { get; set; } = string.Empty;

        public IReadOnlyList<ParameterSchema> RequestParameters => _requestParameters.AsReadOnly();

        public AvroSchema Response { get; set; } = EmptySchema.Value;

        public UnionSchema Error { get; private set; }

        public bool Oneway { get; set; } = false;

        public void AddParameter(ParameterSchema requestParameter)
        {
            if (_requestParameters.Contains(requestParameter))
                throw new AvroException($"Request already contains the parameter: '{requestParameter.Name}'");
            _requestParameters.Add(requestParameter);
        }

        public void AddError(ErrorSchema error)
        {
            if (Error.Contains(error))
                throw new AvroException($"Request already contains the parameter: '{error.FullName}'");
            Error.Add(error);
        }

        public bool Equals(MessageSchema other)
        {
            return other.Name == Name;
        }
    }
}
