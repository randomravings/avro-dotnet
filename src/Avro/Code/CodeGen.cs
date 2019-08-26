using Avro.Schemas;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;

namespace Avro.Code
{
    public partial class CodeGen : IReadOnlyDictionary<string, string>
    {
        private readonly IDictionary<string, MemberDeclarationSyntax> _code;

        public CodeGen()
        {
            _code = new Dictionary<string, MemberDeclarationSyntax>();
        }

        public IDictionary<string, string> Code => _code.ToDictionary(k => k.Key, v => CreateCompileUnit(v.Value).NormalizeWhitespace().ToFullString());

        public IEnumerable<string> Keys => _code.Keys;

        public IEnumerable<string> Values => _code.Values.Select(r => CreateCompileUnit(r).NormalizeWhitespace().ToFullString());

        public int Count => _code.Count;

        public string this[string key] => CreateCompileUnit(_code[key]).NormalizeWhitespace().ToFullString();

        public bool ContainsKey(string key) => _code.ContainsKey(key);

        public bool TryGetValue(string key, out string value)
        {
            if (_code.TryGetValue(key, out var member))
            {
                value = CreateCompileUnit(member).NormalizeWhitespace().ToFullString();
                return true;
            }
            else
            {
                value = null;
                return false;
            }
        }

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            foreach (var kv in _code)
                yield return new KeyValuePair<string, string>(kv.Key, CreateCompileUnit(kv.Value).NormalizeWhitespace().ToFullString());
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        #region Public members

        public void AddSchemas(IEnumerable<AvroSchema> schemas, string parentNamespace = null)
        {
            foreach (var schema in schemas)
                AddSchema(schema, parentNamespace);
        }

        public void AddSchema(AvroSchema schema, string parentNamespace = null)
        {
            if (schema is UnionSchema)
                AddSchemas(schema as UnionSchema, parentNamespace);

            if (schema is NamedSchema)
            {
                var namedSchema = schema as NamedSchema;
                var schemaName = namedSchema.FullName;
                if (string.IsNullOrEmpty(namedSchema.Namespace) && !string.IsNullOrEmpty(parentNamespace))
                    schemaName = $"{parentNamespace}.{schemaName}";
                if (!_code.ContainsKey(namedSchema.FullName))
                    _code.Add(schemaName, CreateCode(namedSchema));

                if (schema is RecordSchema)
                {
                    var recordSchema = schema as RecordSchema;
                    foreach (var fieldSchema in recordSchema)
                        AddSchema(fieldSchema.Type, namedSchema.Namespace);
                }
            }
        }

        public void AddProtocol(AvroProtocol protocol)
        {
            var memberDeclarationSyntax = CreateCode(protocol);
            _code.Add(protocol.FullName, memberDeclarationSyntax);
            AddSchemas(protocol.Types, protocol.Namespace);
        }

        public void WriteCode(TextWriter textWriter)
        {
            var compileUnit = CreateCompileUnit(_code.Values);
            textWriter.Write(
                compileUnit
                .NormalizeWhitespace()
                .ToFullString()
            );
        }

        #endregion

        #region Private static members

        private static MemberDeclarationSyntax CreateCode(NamedSchema schema)
        {
            var schemaTypeName = schema.GetType().Name;
            switch (schemaTypeName)
            {
                case nameof(FixedSchema):
                    return CreateFixedCode(schema as FixedSchema);
                case nameof(EnumSchema):
                    return CreateEnumCode(schema as EnumSchema);
                case nameof(RecordSchema):
                    return CreateRecordCode(schema as RecordSchema, false);
                case nameof(ErrorSchema):
                    return CreateRecordCode(schema as RecordSchema, true);
                default:
                    throw new CodeGenException($"Unsupported Schema: {schemaTypeName}");
            }
        }

        private static MemberDeclarationSyntax CreateCode(AvroProtocol protocol)
        {
            var avro = protocol.ToAvroCanonical();
            var classDeclaration =
                CreateProtocolClass(
                    protocol.Name,
                    avro,
                    protocol.Doc,
                    protocol.Messages
                );

            return
                QualifyMember(
                    classDeclaration,
                    protocol.Namespace
                );
        }

        private static MemberDeclarationSyntax CreateFixedCode(FixedSchema fixedSchema)
        {
            var avro = fixedSchema.ToAvroCanonical();
            var classDeclaration =
                CreateFixedClass(
                    fixedSchema.Name,
                    avro,
                    fixedSchema.Size,
                    fixedSchema.Aliases
                );

            return
                QualifyMember(
                    classDeclaration,
                    fixedSchema.Namespace
                );
        }

        private static MemberDeclarationSyntax CreateEnumCode(EnumSchema enumSchema)
        {
            var enumDeclaration =
                CreateEnum(
                    enumSchema.Name,
                    enumSchema.Symbols,
                    enumSchema.Doc,
                    enumSchema.Aliases
                );

            return
                QualifyMember(
                    enumDeclaration,
                    enumSchema.Namespace
                );
        }

        private static MemberDeclarationSyntax CreateRecordCode(RecordSchema recordSchema, bool isError)
        {
            var avro = recordSchema.ToAvroCanonical();
            var classDeclaration =
                isError ?
                CreateErrorClass(
                    recordSchema.Name,
                    recordSchema.FullName,
                    recordSchema.Count,
                    avro,
                    recordSchema.Doc,
                    recordSchema.Aliases
                ) :
                CreateRecordClass(
                    recordSchema.Name,
                    recordSchema.Count,
                    avro,
                    recordSchema.Doc,
                    recordSchema.Aliases
                )
            ;

            var index = 0;
            var memberDeclarationSyntaxes = new List<MemberDeclarationSyntax>();
            var getSwitchSectionSyntaxes = new List<SwitchSectionSyntax>();
            var setSwitchSectionSyntaxes = new List<SwitchSectionSyntax>();
            foreach (var fieldSchema in recordSchema)
            {
                var propertyName = fieldSchema.Name;
                var propertyType = GetSystemType(fieldSchema.Type);

                memberDeclarationSyntaxes.Add(
                    CreateClassProperty(
                        fieldSchema
                    )
                );

                getSwitchSectionSyntaxes.Add(
                    SwitchCaseGetProperty(
                        index,
                        propertyName
                    )
                );

                setSwitchSectionSyntaxes.Add(
                    SwitchCaseSetProperty(
                        index,
                        propertyName,
                        propertyType
                    )
                );

                index++;
            }

            memberDeclarationSyntaxes.Add(
                CreateRecordClassIndexer(
                    getSwitchSectionSyntaxes,
                    setSwitchSectionSyntaxes,
                    getSwitchSectionSyntaxes.Count() - 1
                )
            );

            classDeclaration =
                AddMembersToClass(
                    classDeclaration,
                    memberDeclarationSyntaxes.ToArray()
                );

            return
                QualifyMember(
                    classDeclaration,
                    recordSchema.Namespace
                );
        }

        #endregion

        #region Public static members

        public static string GetCode(NamedSchema schema)
        {
            var codeGen = new CodeGen();
            codeGen.AddSchema(schema);
            var codeBuilder = new StringBuilder();
            using (var codeWriter = new StringWriter(codeBuilder))
                codeGen.WriteCode(codeWriter);
            return codeBuilder.ToString();
        }

        public static Assembly Compile(string assemblyName, NamedSchema schema, out XmlDocument xmlDocumentation)
        {
            var code = GetCode(schema);
            return Compile(assemblyName, code, out xmlDocumentation);
        }

        #endregion
    }
}
