using Avro.Code;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

/// <summary>
/// 
/// </summary>
namespace Avro
{
    public class AvroGen
    {
        static readonly string assmbly = typeof(AvroGen).Assembly.GetName().Name;
        static readonly string version = typeof(AvroGen).Assembly.GetName().Version.ToString();
        static readonly string program = AppDomain.CurrentDomain.FriendlyName;

        static void Main(string[] args)
        {
            // Print usage if no arguments provided or help requested
            if (args.Length == 0 || args[0] == "-h" || args[0] == "--help")
            {
                Usage();
                return;
            }

            switch(args.FirstOrDefault())
            {
                case null:
                case "-h":
                case "--help":
                    Help();
                    break;
                case "-v":
                case "--version":
                    Version();
                    break;
                case "-s":
                case "--schema":
                    break;
                case "-p":
                case "--protocol":
                    break;
                default:
                    Usage();
                    break;
            }

            // Parse command line arguments
            bool? isProtocol = null;
            string inputFile = null;
            string outputDir = null;
            var namespaceMapping = new Dictionary<string, string>();
            for (int i = 0; i < args.Length; ++i)
            {
                if (args[i] == "-p")
                {
                    if (i + 1 >= args.Length)
                    {
                        Console.WriteLine("Missing path to protocol file");
                        Usage();
                        return;
                    }

                    isProtocol = true;
                    inputFile = args[++i];
                }
                else if (args[i] == "-s")
                {
                    if (i + 1 >= args.Length)
                    {
                        Console.WriteLine("Missing path to schema file");
                        Usage();
                        return;
                    }

                    isProtocol = false;
                    inputFile = args[++i];
                }
                else if (args[i] == "--namespace")
                {
                    if (i + 1 >= args.Length)
                    {
                        Console.WriteLine("Missing namespace mapping");
                        Usage();
                        return;
                    }

                    var parts = args[++i].Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length != 2)
                    {
                        Console.WriteLine("Malformed namespace mapping. Required format is \"avro.namespace:csharp.namespace\"");
                        Usage();
                        return;
                    }

                    namespaceMapping[parts[0]] = parts[1];
                }
                else if (outputDir == null)
                {
                    outputDir = args[i];
                }
                else
                {
                    Console.WriteLine("Unexpected command line argument: {0}", args[i]);
                    Usage();
                }
            }

            

            // Ensure we got all the command line arguments we need
            bool isValid = true;
            if (!isProtocol.HasValue || inputFile == null)
            {
                Console.WriteLine("Must provide either '-p <protocolfile>' or '-s <schemafile>'");
                isValid = false;
            }
            else if (outputDir == null)
            {
                Console.WriteLine("Must provide 'outputdir'");
                isValid = false;
            }

            if (!isValid)
            {
                Usage();
                return;
            }

            else if (isProtocol.Value)
            {
                GenProtocol(inputFile, outputDir, namespaceMapping);
                return;
            }

            else
            {
                var files = GetFiles(inputFile, ".avsc");
                GenSchema(files, outputDir, namespaceMapping);
            }
        }

        static void Version()
        {
            Console.WriteLine();
            Console.WriteLine($"{assmbly} ({version})");
            Console.WriteLine();
        }

        static void Usage()
        {
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine($"Usage:");
            Console.WriteLine($"  {program} [options]");
            Console.WriteLine(); 
            Console.WriteLine($"Options:");
            Console.WriteLine($"  -h|--help      Display help.");
            Console.WriteLine($"  -v|--version   Display installed {program} version.");
            Console.WriteLine();
            return;
        }

        static void Help()
        {
            Console.WriteLine($"  {program} -s | --schema | -p | --protocol <path> <outdir> [options]");
            Console.WriteLine($"  --namespace Map an Avro schema/protocol namespace to a C# namespace.");
            Console.WriteLine($"              The format is \"my.avro.namespace:my.csharp.namespace\".");
            Console.WriteLine($"              May be specified multiple times to map multiple namespaces.");
        }

        static IEnumerable<FileInfo> GetFiles(string fileOrDirectory, string fileExt)
        {
            if (File.Exists(fileOrDirectory) || Path.GetExtension(fileOrDirectory) == fileExt)
                return new FileInfo[] { new FileInfo(fileOrDirectory) };
            if (Directory.Exists(fileOrDirectory))
                return Directory.GetFiles(fileOrDirectory, $"*{fileExt}").Select(r => new FileInfo(r));
            return null;
        }

        static void GenProtocol(string infile, string outdir, IEnumerable<KeyValuePair<string, string>> namespaceMapping)
        {
            //try
            //{
            //    string text = System.IO.File.ReadAllText(infile);
            //    Protocol protocol = Protocol.Parse(text);

            //    CodeGen codegen = new CodeGen();
            //    codegen.AddProtocol(protocol);

            //    foreach (var entry in namespaceMapping)
            //        codegen.NamespaceMapping[entry.Key] = entry.Value;

            //    codegen.GenerateCode();
            //    codegen.WriteTypes(outdir);
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine("Exception occurred. " + ex.Message);
            //}
        }

        static void GenSchema(IEnumerable<FileInfo> files, string outdir, IEnumerable<KeyValuePair<string, string>> nsMap)
        {
            try
            {
                var codeGen = new CodeGen();
                var codeWriter = new CodeWriter();

                foreach (var file in files)
                {
                    using (var reader = new StreamReader(file.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite), Encoding.UTF8))
                    {
                        var text = reader.ReadToEnd();
                        var schema = AvroParser.ReadSchema(text, out var schemas);
                        codeGen.AddSchemas(schemas);
                    }
                }
                //foreach (var map in nsMap)
                //    codegen.NamespaceMapping[map.Key] = map.Value;
                codeWriter.WriteProject(outdir, codeGen);

            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception occurred. " + ex.Message);
            }
        }
    }
}
