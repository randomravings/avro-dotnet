using Avro.Schemas;
using Avro.Utils;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;

namespace Avro.CodeDom
{
    public class CodeGen
    {
        private readonly IDictionary<string, MemberDeclarationSyntax> _code;

        public IEnumerable<string> Code => _code.Values.Select(r => r.ToFullString());

        public CodeGen()
        {
            _code = new Dictionary<string, MemberDeclarationSyntax>();
        }

        #region Public members

        public void AddSchema(Schema schema, string parentNamespace = null)
        {
            if (schema is UnionSchema)
            {
                var unionSchema = schema as UnionSchema;
                foreach (var item in unionSchema)
                    AddSchema(schema, parentNamespace);
            }

            if (schema is NamedSchema)
            {
                var namedSchema = schema as NamedSchema;
                if (!_code.ContainsKey(namedSchema.FullName))
                    _code.Add(namedSchema.FullName, CreateCode(namedSchema, parentNamespace));
            }

            if (schema is RecordSchema)
            {
                var recordSchema = schema as RecordSchema;
                foreach (var fieldSchema in recordSchema)
                    AddSchema(fieldSchema.Type, parentNamespace);
            }
        }

        public void AddProtocol(Protocol protocol)
        {
            if (_code.ContainsKey(protocol.FullName))
                throw new CodeGenException($"There is already an item with name: {protocol.FullName}");
            var memberDeclarationSyntax = CreateCode(protocol);
            _code.Add(protocol.FullName, memberDeclarationSyntax);
        }

        public void WriteCode(TextWriter textWriter, string name = null)
        {
            var compileUnit = CreateCompileUnit(name);
            textWriter.Write(
                compileUnit
                .NormalizeWhitespace()
                .ToFullString()
            );
        }

        public string GetCode(string name = null)
        {
            var compileUnit = CreateCompileUnit(name);
            return
                compileUnit
                .NormalizeWhitespace()
                .ToFullString()
            ;
        }

        #endregion

        #region Private members

        private CompilationUnitSyntax CreateCompileUnit(string name = null)
        {
            return
                name == null ?
                TypeGenUtil.CreateCompileUnit(_code.Values) :
                TypeGenUtil.CreateCompileUnit(_code[name])
            ;
        }

        #endregion

        #region Private static members

        private static MemberDeclarationSyntax CreateCode(NamedSchema schema, string parentNamespace = null)
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

        private static MemberDeclarationSyntax CreateCode(Protocol protocol)
        {
            return null;
        }

        private static MemberDeclarationSyntax CreateFixedCode(FixedSchema fixedSchema)
        {
            var avro = new StringBuilder();
            using (var writer = new StringWriter(avro))
                SchemaWriter.WriteCanonical(writer, fixedSchema);
            var classDeclaration =
                TypeGenUtil.CreateFixedClass(
                    fixedSchema.Name,
                    avro.ToString(),
                    fixedSchema.Size
                );

            return
                TypeGenUtil.QualifyMember(
                    classDeclaration,
                    fixedSchema.Namespace
                );
        }

        private static MemberDeclarationSyntax CreateEnumCode(EnumSchema enumSchema)
        {
            var enumDeclaration =
                TypeGenUtil.CreateEnum(
                    enumSchema.Name,
                    enumSchema.Symbols,
                    enumSchema.Doc
                );

            return
                TypeGenUtil.QualifyMember(
                    enumDeclaration,
                    enumSchema.Namespace
                );
        }

        private static MemberDeclarationSyntax CreateRecordCode(RecordSchema recordSchema, bool isError)
        {
            var avro = new StringBuilder();
            using (var writer = new StringWriter(avro))
                SchemaWriter.WriteCanonical(writer, recordSchema);
            var classDeclaration =
                isError ?
                TypeGenUtil.CreateErrorClass(
                    recordSchema.Name,
                    recordSchema.Count,
                    avro.ToString(),
                    recordSchema.Doc
                ) :
                TypeGenUtil.CreateRecordClass(
                    recordSchema.Name,
                    recordSchema.Count,
                    avro.ToString(),
                    recordSchema.Doc
                )
            ;

            var index = 0;
            var memberDeclarationSyntaxes = new List<MemberDeclarationSyntax>();
            var getSwitchSectionSyntaxes = new List<SwitchSectionSyntax>();
            var setSwitchSectionSyntaxes = new List<SwitchSectionSyntax>();
            foreach (var fieldSchema in recordSchema)
            {
                var propertyName = fieldSchema.Name;
                var propertyType = TypeGenUtil.GetSystemType(fieldSchema.Type);

                memberDeclarationSyntaxes.Add(
                    TypeGenUtil.CreateClassProperty(
                        propertyName,
                        propertyType,
                        fieldSchema.Doc
                    )
                );

                getSwitchSectionSyntaxes.Add(
                    TypeGenUtil.SwitchCaseGetProperty(
                        index.ToString(),
                        propertyName
                    )
                );

                setSwitchSectionSyntaxes.Add(
                    TypeGenUtil.SwitchCaseSetProperty(
                        index.ToString(),
                        propertyName,
                        propertyType
                    )
                );

                index++;
            }

            memberDeclarationSyntaxes.Add(
                TypeGenUtil.CreateClassGetMethod(
                    getSwitchSectionSyntaxes,
                    getSwitchSectionSyntaxes.Count() - 1
                )
            );

            memberDeclarationSyntaxes.Add(
                TypeGenUtil.CreateClassSetMethod(
                    setSwitchSectionSyntaxes,
                    setSwitchSectionSyntaxes.Count() - 1
                )
            );

            classDeclaration =
                TypeGenUtil.AddMembersToClass(
                    classDeclaration,
                    memberDeclarationSyntaxes.ToArray()
                );

            return
                TypeGenUtil.QualifyMember(
                    classDeclaration,
                    recordSchema.Namespace
                );
        }

        private static void WriteCode(MemberDeclarationSyntax memberDeclarationSyntax, TextWriter textWriter)
        {
            var compileUnit = TypeGenUtil.CreateCompileUnit(memberDeclarationSyntax);
            textWriter.Write(
                compileUnit
                .NormalizeWhitespace()
                .ToFullString()
            );
        }

        #endregion

        #region Public static members

        public static string GetCode(IEnumerable<NamedSchema> schemas)
        {
            var codeGen = new CodeGen();
            foreach (var schema in schemas)
                codeGen.AddSchema(schema);
            var codeBuilder = new StringBuilder();
            using (var codeWriter = new StringWriter(codeBuilder))
                codeGen.WriteCode(codeWriter);
            return codeBuilder.ToString();
        }

        public static string GetCode(NamedSchema schema)
        {
            var codeGen = new CodeGen();
            codeGen.AddSchema(schema);
            var codeBuilder = new StringBuilder();
            using (var codeWriter = new StringWriter(codeBuilder))
                codeGen.WriteCode(codeWriter);
            return codeBuilder.ToString();
        }

        public static void WriteCode(IEnumerable<NamedSchema> schemas, TextWriter textWriter)
        {
            foreach (var schema in schemas)
            {
                var code = CreateCode(schema);
                textWriter.WriteLine(code.ToFullString());
            }
        }

        public static void WriteCode(NamedSchema schema, TextWriter textWriter)
        {
            var code = CreateCode(schema);
            textWriter.WriteLine(code.ToFullString());
        }

        public void CreateProject(string directory)
        {
            var directoryInfo = Directory.CreateDirectory(directory);
            var projectFile = Path.Combine(directoryInfo.FullName, $"{directoryInfo.Name}.csproj");

            foreach (var item in _code)
            {
                var path = item.Key.Replace('.', Path.DirectorySeparatorChar);
                path = Path.Combine(directory, path);
                path = Path.ChangeExtension(path, "cs");
                Directory.CreateDirectory(Path.GetDirectoryName(path));

                using (var stringWriter = new StreamWriter(File.Open(path, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite)))
                    WriteCode(item.Value, stringWriter);
            }

            using (var stringWriter = new StreamWriter(File.Open(projectFile, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite)))
            {
                stringWriter.WriteLine($"<Project Sdk=\"Microsoft.NET.Sdk\">");
                stringWriter.WriteLine($"  <PropertyGroup>");
                stringWriter.WriteLine($"    <TargetFramework>netcoreapp2.2</TargetFramework>");
                stringWriter.WriteLine($"  </PropertyGroup>");
                stringWriter.WriteLine($"  <ItemGroup>");
                stringWriter.WriteLine($"    <PackageReference Include=\"Avro\" />");
                stringWriter.WriteLine($"  </ItemGroup>");
                stringWriter.WriteLine($"</Project>");
            }
        }

        public static Type CreateType(NamedSchema schema, string assemblyName = null)
        {
            var code = GetCode(schema);
            var assembly = TypeGenUtil.Compile(assemblyName ?? Guid.NewGuid().ToString(), code, out _);
            return assembly.ExportedTypes.FirstOrDefault(r => r.FullName == schema.FullName);
        }

        public static Assembly Compile(string assemblyName, NamedSchema schema, out XmlDocument xmlDocumentation)
        {
            var code = GetCode(schema);
            return TypeGenUtil.Compile(assemblyName, code, out xmlDocumentation);
        }

        public static Assembly Compile(string assemblyName, IEnumerable<NamedSchema> schemas, out XmlDocument xmlDocumentation)
        {
            var code = GetCode(schemas);
            return TypeGenUtil.Compile(assemblyName, code, out xmlDocumentation);
        }

        #endregion
    }
}
