using Microsoft.Extensions.DependencyModel;
using Avro.Schema;
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
using System;

namespace Avro.Code
{
    public partial class CodeGen : IReadOnlyDictionary<string, string>
    {
        private readonly IDictionary<string, MemberDeclarationSyntax> _code;
        private readonly IDictionary<string, string> _nsMap;

        public CodeGen(IDictionary<string, string> nsMap)
        {
            _code = new Dictionary<string, MemberDeclarationSyntax>();
            _nsMap = nsMap;
        }

        public IDictionary<string, string> Code => _code.ToDictionary(k => k.Key, v => SyntaxGenerator.CreateCompileUnit(v.Value).NormalizeWhitespace().ToFullString());

        public IEnumerable<string> Keys => _code.Keys;

        public IEnumerable<string> Values => _code.Values.Select(r => SyntaxGenerator.CreateCompileUnit(r).NormalizeWhitespace().ToFullString());

        public int Count => _code.Count;

        public string this[string key] => SyntaxGenerator.CreateCompileUnit(_code[key]).NormalizeWhitespace().ToFullString();

        public bool ContainsKey(string key) => _code.ContainsKey(key);

        public bool TryGetValue(string key, out string value)
        {
            if (_code.TryGetValue(key, out var member))
            {
                value = SyntaxGenerator.CreateCompileUnit(member).NormalizeWhitespace().ToFullString();
                return true;
            }
            else
            {
                value = string.Empty;
                return false;
            }
        }

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            foreach (var kv in _code)
                yield return new KeyValuePair<string, string>(kv.Key, SyntaxGenerator.CreateCompileUnit(kv.Value).NormalizeWhitespace().ToFullString());
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        #region Public members

        public void AddSchemas(IEnumerable<AvroSchema> schemas)
        {
            foreach (var schema in schemas)
                AddSchema(schema);
        }

        public void AddSchema(AvroSchema schema)
        {
            if (schema is UnionSchema)
                AddSchemas((UnionSchema)schema);

            if (schema is NamedSchema)
            {
                var schemaName = ((NamedSchema)schema).FullName;
                if (!_code.ContainsKey(((NamedSchema)schema).FullName))
                    _code.Add(schemaName, CreateCode((NamedSchema)schema, _nsMap));

                if (schema is RecordSchema recordSchema)
                    foreach (var fieldSchema in recordSchema)
                        AddSchema(fieldSchema.Type);
            }
        }

        public void AddProtocol(AvroProtocol protocol)
        {
            var memberDeclarationSyntax = CreateCode(protocol);
            _code.Add(protocol.FullName, memberDeclarationSyntax);
            AddSchemas(protocol.Types);
        }

        public void WriteCode(TextWriter textWriter)
        {
            var compileUnit = SyntaxGenerator.CreateCompileUnit(_code.Values);
            textWriter.Write(
                compileUnit
                .NormalizeWhitespace()
                .ToFullString()
            );
        }

        #endregion

        #region Private static members

        private static MemberDeclarationSyntax CreateCode(NamedSchema schema, IDictionary<string, string> nsMap)
        {
            var ns = schema.Namespace;
            var trn = nsMap.OrderByDescending(r => r.Key)
                .Where(r => ns.StartsWith(r.Key))
                .Select(r => ($"{r.Value}{ns.Substring(r.Key.Length)}"))
                .DefaultIfEmpty(string.Empty)
                .FirstOrDefault()
            ;

            return schema switch
            {
                FixedSchema s => CreateFixedCode(s, ns),
                EnumSchema s => CreateEnumCode(s, ns),
                ErrorSchema s => CreateErrorCode(s, ns),
                RecordSchema s => CreateRecordCode(s, ns),
                _ => throw new NotSupportedException($"Unsupported Schema: '{schema.ToString()}'"),
            };
        }

        private static MemberDeclarationSyntax CreateCode(AvroProtocol protocol)
        {
            var avro = protocol.ToAvroCanonical();
            var classDeclaration =
                SyntaxGenerator.CreateProtocolClass(
                    protocol.Name,
                    avro,
                    protocol.Doc,
                    protocol.Messages
                );

            return
                SyntaxGenerator.QualifyMember(
                    classDeclaration,
                    protocol.Namespace
                );
        }

        private static MemberDeclarationSyntax CreateFixedCode(FixedSchema fixedSchema, string ns)
        {
            var avro = fixedSchema.ToAvroCanonical();
            var classDeclaration =
                SyntaxGenerator.CreateFixedClass(
                    ns,
                    fixedSchema.Name,
                    avro,
                    fixedSchema.Size,
                    fixedSchema.Aliases
                );

            return
                SyntaxGenerator.QualifyMember(
                    classDeclaration,
                    fixedSchema.Namespace
                );
        }

        private static MemberDeclarationSyntax CreateEnumCode(EnumSchema enumSchema, string ns)
        {
            var enumDeclaration =
                SyntaxGenerator.CreateEnum(
                    ns,
                    enumSchema.Name,
                    enumSchema.Keys,
                    enumSchema.Doc,
                    enumSchema.Aliases
                );

            return enumDeclaration;
        }

        private static MemberDeclarationSyntax CreateErrorCode(ErrorSchema recordSchema, string ns)
        {
            var avro = recordSchema.ToAvroCanonical();
            var classDeclaration = SyntaxGenerator.CreateErrorClass(
                ns,
                recordSchema.Name,
                recordSchema.FullName,
                recordSchema.Count,
                avro,
                recordSchema.Doc,
                recordSchema.Aliases
            );

            var index = 0;
            var memberDeclarationSyntaxes = new List<MemberDeclarationSyntax>();
            var getSwitchSectionSyntaxes = new List<SwitchSectionSyntax>();
            var setSwitchSectionSyntaxes = new List<SwitchSectionSyntax>();
            foreach (var fieldSchema in recordSchema)
            {
                var propertyName = fieldSchema.Name;
                var propertyType = SyntaxGenerator.GetSystemType(fieldSchema.Type);

                memberDeclarationSyntaxes.Add(
                    SyntaxGenerator.CreateClassProperty(
                        fieldSchema
                    )
                );

                getSwitchSectionSyntaxes.Add(
                    SyntaxGenerator.SwitchCaseGetProperty(
                        index,
                        propertyName
                    )
                );

                setSwitchSectionSyntaxes.Add(
                    SyntaxGenerator.SwitchCaseSetProperty(
                        index,
                        propertyName,
                        propertyType
                    )
                );

                index++;
            }

            memberDeclarationSyntaxes.Add(
                SyntaxGenerator.CreateRecordClassIndexer(
                    getSwitchSectionSyntaxes,
                    setSwitchSectionSyntaxes,
                    getSwitchSectionSyntaxes.Count() - 1
                )
            );

            classDeclaration =
                SyntaxGenerator.AddMembersToClass(
                    classDeclaration,
                    memberDeclarationSyntaxes.ToArray()
                );

            return
                SyntaxGenerator.QualifyMember(
                    classDeclaration,
                    recordSchema.Namespace
                );
        }

        private static MemberDeclarationSyntax CreateRecordCode(RecordSchema recordSchema, string ns)
        {
            var avro = recordSchema.ToAvroCanonical();
            var classDeclaration = SyntaxGenerator.CreateRecordClass(
                ns,
                recordSchema.Name,
                recordSchema.Count,
                avro,
                recordSchema.Doc,
                recordSchema.Aliases
            );

            var index = 0;
            var memberDeclarationSyntaxes = new List<MemberDeclarationSyntax>();
            var getSwitchSectionSyntaxes = new List<SwitchSectionSyntax>();
            var setSwitchSectionSyntaxes = new List<SwitchSectionSyntax>();
            foreach (var fieldSchema in recordSchema)
            {
                var propertyName = fieldSchema.Name;
                var propertyType = SyntaxGenerator.GetSystemType(fieldSchema.Type);

                memberDeclarationSyntaxes.Add(
                    SyntaxGenerator.CreateClassProperty(
                        fieldSchema
                    )
                );

                getSwitchSectionSyntaxes.Add(
                    SyntaxGenerator.SwitchCaseGetProperty(
                        index,
                        propertyName
                    )
                );

                setSwitchSectionSyntaxes.Add(
                    SyntaxGenerator.SwitchCaseSetProperty(
                        index,
                        propertyName,
                        propertyType
                    )
                );

                index++;
            }

            memberDeclarationSyntaxes.Add(
                SyntaxGenerator.CreateRecordClassIndexer(
                    getSwitchSectionSyntaxes,
                    setSwitchSectionSyntaxes,
                    getSwitchSectionSyntaxes.Count() - 1
                )
            );

            classDeclaration =
                SyntaxGenerator.AddMembersToClass(
                    classDeclaration,
                    memberDeclarationSyntaxes.ToArray()
                );

            return
                SyntaxGenerator.QualifyMember(
                    classDeclaration,
                    recordSchema.Namespace
                );
        }

        #endregion

        #region Public static members

        public static string GetCode(NamedSchema schema)
        {
            var codeGen = new CodeGen(new Dictionary<string, string>());
            codeGen.AddSchema(schema);
            var codeBuilder = new StringBuilder();
            using (var codeWriter = new StringWriter(codeBuilder))
                codeGen.WriteCode(codeWriter);
            return codeBuilder.ToString();
        }

        public static Assembly Compile(string assemblyName, NamedSchema schema, out XmlDocument xmlDocumentation)
        {
            var code = GetCode(schema);
            return SyntaxGenerator.Compile(assemblyName, code, out xmlDocumentation);
        }

        #endregion
    }
}
