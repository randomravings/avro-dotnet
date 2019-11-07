using Avro.Code;
using CommandLine;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Avro
{
    public class AvroGen
    {
        private static readonly MD5 HASH_FUNC = MD5.Create();

        static int Main(string[] args)
        {
            return Parser.Default.ParseArguments<NewOption, AddOption, ExampleOptions>(args)
            .MapResult(
                (NewOption opts) => GenerateCode(opts.Project, opts),
                (AddOption opts) => GenerateCode(string.Empty, opts),
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

        private static void Log(string entry, bool quiet, int newLineCount = 0)
        {
            if(quiet)
                return;
            Console.WriteLine(entry);
            for(int i = 0; i < newLineCount; i++)
                Console.WriteLine();
        }

        static IEnumerable<FileInfo> GetFiles(string fileOrDirectory, string fileExt)
        {
            if (File.Exists(fileOrDirectory) || Path.GetExtension(fileOrDirectory) == fileExt)
                return new FileInfo[] { new FileInfo(fileOrDirectory) };
            if (Directory.Exists(fileOrDirectory))
                return Directory.GetFiles(fileOrDirectory, $"*{fileExt}").Select(r => new FileInfo(r));
            return new FileInfo[0];
        }

        static int GenerateCode(string projectName, DefaultOption options)
        {
            var namespaceMapping = options.Namespace.Select(r => r.Split('.')).ToDictionary(k => k[0], v => v[1]);

            var codeGen = new CodeGen(namespaceMapping);
            var codeWriter = new CodeWriter();

            var protocolHashes = new Dictionary<string, string>();
            var typeHashes = new Dictionary<string, string>();

            if (options.Schema)
            {
                Log("Parsing Schema File(s) ...", options.Quiet);
                foreach (var schemaFile in GetFiles(options.Path, ".avsc"))
                {
                    Log($"    '{schemaFile.FullName}' ...", options.Quiet);
                    using var reader = new StreamReader(schemaFile.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite), Encoding.UTF8);

                    var text = reader.ReadToEnd();
                    var schema = AvroParser.ReadSchema(text, out var schemas);
                    codeGen.AddSchemas(schemas);
                    foreach (var s in schemas)
                    {
                        var hash = string.Join("", HASH_FUNC.ComputeHash(Encoding.UTF8.GetBytes(s.ToAvroCanonical())).Select(r => r.ToString("X2")));
                        if (typeHashes.TryGetValue(s.Name, out var existingHash))
                            typeHashes.Add(s.Name, hash);
                        else
                            if (hash != existingHash)
                            Log("           Hash mismatch:", options.Quiet);
                        Log($"        T: '{s.FullName}'", options.Quiet);
                    }
                }
            }

            if (options.Protocol)
            {
                Log("Parsing Schema File(s) ...", options.Quiet);
                foreach (var protocolFile in GetFiles(options.Path, ".avpr"))
                {
                    Log($"    '{protocolFile.FullName}' ...", options.Quiet);
                    using var reader = new StreamReader(protocolFile.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite), Encoding.UTF8);

                    var text = reader.ReadToEnd();
                    var protocol = AvroParser.ReadProtocol(text, out var schemas);
                    codeGen.AddProtocol(protocol);
                    Log($"        P: '{protocol.FullName}'", options.Quiet);
                    foreach (var s in schemas)
                        Log($"        T: '{s.FullName}'", options.Quiet);
                }
            }

            codeWriter.WriteProject(options.OutDir, codeGen, projectName);
            return 0;
        }
    }

    public class DefaultOption
    {

        [Value(0, MetaName = "path", Required = true, HelpText = "Source file or directory.")]
        public string Path { get; set; } = string.Empty;

        [Value(1, MetaName = "outdir", Required = true, HelpText = "Target root directory for code bindings.")]

        public string OutDir { get; set; } = string.Empty;
        [Option('s', "schema", Required = false, HelpText = "Add files with .avsc extension.")]
        public bool Schema { get; set; }

        [Option('p', "protocol", Required = false, HelpText = "Add files with .avpr extension.")]
        public bool Protocol { get; set; }

        [Option('n', "namespace", Required = false, HelpText = "Avro to C# namespace translations as colon separated key-values: (<x.y>:<y.x> a.b:x>). Namespaces are sored by the longest sequence meanging that <x.y.z> will be applied before <x.y> etc.")]
        public IEnumerable<string> Namespace { get; set; } = new List<string>();

        [Option('q', "quiet", Required = false, HelpText = "Run in Quiet mode")]
        public bool Quiet { get; set; }
    }

    [Verb("new", HelpText = "Add code binding to outdir and create a C# project.")]
    public class NewOption : DefaultOption
    {
        [Option('c', "project", HelpText = "C# project file without extension. If omitted the target directory name is used.")]
        public string Project { get; set; } = string.Empty;
    }

    [Verb("add", HelpText = "Add code binding to outdir.")]
    public class AddOption : DefaultOption { }

    [Verb("example", HelpText = "Display example usage.")]
    public class ExampleOptions { }
}
