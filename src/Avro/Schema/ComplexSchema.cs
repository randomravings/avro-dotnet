namespace Avro.Schema
{
    public abstract class ComplexSchema : NamedSchema
    {
        public ComplexSchema()
            : base() { }

        public ComplexSchema(string name)
            : base(name) { }

        public ComplexSchema(string name, string ns)
            : base(name, ns) { }

        public string Doc { get; set; }
    }
}
