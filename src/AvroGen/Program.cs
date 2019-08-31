using Avro.Code;
using CommandLine;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Avro
{
    public class AvroGen
    {
        static int Main(string[] args)
        {
            return Parser.Default.ParseArguments<NewOptions, AddOptions, ExampleOptions>(args)
            .MapResult(
                (NewOptions opts) => GenerateCode(opts.Project, opts),
                (AddOptions opts) => GenerateCode(null, opts),
                (ExampleOptions ops) => ShowExample(),
                errs => 1
            ); ;
        }

        private static int ShowExample()
        {
            Console.WriteLine();
            Console.WriteLine($"Create a new project from a single Avro schema file:");
            Console.WriteLine($"> avrogen new avrofiles\\schema.avsc workspace\\avro --project AvroContracts -s");
            Console.WriteLine();
            Console.WriteLine($"Add all protocols found in .avpr files on source folder to target folder:");
            Console.WriteLine($"> avrogen add avrofiles\\protocols workspace\\avro --project AvroProtocols -p");
            Console.WriteLine();

            return 0;
        }

        static IEnumerable<FileInfo> GetFiles(string fileOrDirectory, string fileExt)
        {
            if (File.Exists(fileOrDirectory) || Path.GetExtension(fileOrDirectory) == fileExt)
                return new FileInfo[] { new FileInfo(fileOrDirectory) };
            if (Directory.Exists(fileOrDirectory))
                return Directory.GetFiles(fileOrDirectory, $"*{fileExt}").Select(r => new FileInfo(r));
            return null;
        }

        static int GenerateCode(string projectName, Options options)
        {
            var namespaceMapping = options.Namespace.Select(r => r.Split('.')).ToDictionary(k => k[0], v => v[1]);

            var codeGen = new CodeGen(namespaceMapping);
            var codeWriter = new CodeWriter();

            if (options.Schema)
                foreach (var schemaFile in GetFiles(options.Path, ".avsc"))
                {
                    using (var reader = new StreamReader(schemaFile.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite), Encoding.UTF8))
                    {
                        var text = reader.ReadToEnd();
                        var schema = AvroParser.ReadSchema(text, out var schemas);
                        codeGen.AddSchemas(schemas);
                    }
                }

            if (options.Protocol)
                foreach (var protocolFile in GetFiles(options.Path, ".avpr"))
                {
                    using (var reader = new StreamReader(protocolFile.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite), Encoding.UTF8))
                    {
                        var text = reader.ReadToEnd();
                        var protocol = AvroParser.ReadProtocol(text);
                        codeGen.AddProtocol(protocol);
                    }
                }

            codeWriter.WriteProject(options.OutDir, codeGen, projectName);
            return 0;
        }

        public class Options
        {

            [Value(0, MetaName = "path", Required = true, HelpText = "Source file or directory.")]
            public string Path { get; set; }

            [Value(1, MetaName = "outdir", Required = true, HelpText = "Target root directory for code bindings.")]

            public string OutDir { get; set; }
            [Option('s', "schema", Required = false, HelpText = "Add files with .avsc extension.")]
            public bool Schema { get; set; }

            [Option('p', "protocol", Required = false, HelpText = "Add files with .avpr extension.")]
            public bool Protocol { get; set; }

            [Option('n', "namespace", Required = false, HelpText = "Avro to C# namespace translations as colon separated key-values: (<x.y>:<y.x> <a.b:x>). Namespaces are sored by the longest sequence meanging that <x.y.z> will be applied before <x.y> etc.")]
            public IEnumerable<string> Namespace{ get; set; }
        }

        [Verb("new", HelpText = "Add code binding to outdir and create a C# project.")]
        public class NewOptions : Options
        {
            [Option('c', "project", HelpText = "C# project file without extension. If omitted the target directory name is used.")]
            public string Project { get; set; }
        }

        [Verb("add", HelpText = "Add code binding to outdir.")]
        public class AddOptions : Options { }

        [Verb("example", HelpText = "Display example usage.")]
        public class ExampleOptions { }
    }
}
