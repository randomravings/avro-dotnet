using Avro.Utils;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;

namespace Avro.Schema
{
    public class RecordFieldSchema : AvroSchema, IEquatable<RecordFieldSchema>
    {
        private string _name = string.Empty;
        private IList<string> _aliases = new List<string>();
        private JToken _default = JsonUtil.EmptyDefault;

        public RecordFieldSchema()
            : base()
        {
            Aliases = new List<string>();
        }

        public RecordFieldSchema(string name)
            : base()
        {
            Name = name;
        }

        public RecordFieldSchema(string name, AvroSchema type)
            : base()
        {
            Name = name;
            Type = type;
        }

        public string Name { get { return _name; } set { NameValidator.ValidateName(value); _name = value; } }

        public AvroSchema Type { get; set; } = EmptySchema.Value;

        public string Order { get; set; } = "ignore";
        public JToken Default { get { return _default; } set { DefaultValidator.ValidateJson(Type, value); _default = value; } }
        public string Doc { get; set; } = string.Empty;
        public IList<string> Aliases { get { return _aliases; } set { NameValidator.ValidateNames(value); _aliases = value; } }

        public bool Equals(RecordFieldSchema other) => string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);

        public override bool Equals(object obj) => Equals((RecordFieldSchema)obj);
        public override int GetHashCode() => base.GetHashCode();
    }
}
