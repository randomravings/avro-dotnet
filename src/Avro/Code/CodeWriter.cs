using System.IO;
using System.IO.Abstractions;

namespace Avro.Code
{
    public class CodeWriter
    {
        private readonly IFileSystem _fileSystem;

        public CodeWriter()
        : this(new FileSystem()) { }

        public CodeWriter(IFileSystem fileSystem)
        {
            _fileSystem = fileSystem;
        }

        public void WriteProject(string rootDirectory, CodeGen codeGen, string projectName)
        {
            var directoryInfo = _fileSystem.Directory.CreateDirectory(rootDirectory);

            foreach (var item in codeGen.Code)
            {
                var path = item.Key.Replace('.', Path.DirectorySeparatorChar);
                path = Path.Combine(rootDirectory, path);
                path = Path.ChangeExtension(path, "cs");
                _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(path));

                using (var streamWriter = new StreamWriter(_fileSystem.File.Open(path, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite)))
                    streamWriter.Write(item.Value);
            }

            if (!string.IsNullOrEmpty(projectName))
            {
                var assmbly = typeof(CodeWriter).Assembly;
                var referenceName = assmbly.GetName().Name;
                var referenceVersion = assmbly.GetName().Version.ToString();
                var projectFile = Path.Combine(directoryInfo.FullName, $"{projectName}.csproj");
                using var stringWriter = new StreamWriter(_fileSystem.File.Open(projectFile, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite));
                stringWriter.WriteLine($"<Project Sdk=\"Microsoft.NET.Sdk\">");
                stringWriter.WriteLine($"  <PropertyGroup>");
                stringWriter.WriteLine($"    <TargetFramework>netcoreapp2.2</TargetFramework>");
                stringWriter.WriteLine($"  </PropertyGroup>");
                stringWriter.WriteLine($"  <ItemGroup>");
                stringWriter.WriteLine($"    <PackageReference Include=\"{referenceName}\" Version=\"{referenceVersion}\" />");
                stringWriter.WriteLine($"  </ItemGroup>");
                stringWriter.WriteLine($"</Project>");
            }
        }
    }
}
