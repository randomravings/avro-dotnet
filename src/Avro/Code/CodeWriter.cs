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

        public void WriteProject(string rootDirectory, CodeGen codeGen)
        {
            var directoryInfo = _fileSystem.Directory.CreateDirectory(rootDirectory);
            var projectFile = Path.Combine(directoryInfo.FullName, $"{directoryInfo.Name}.csproj");

            foreach (var item in codeGen.Code)
            {
                var path = item.Key.Replace('.', Path.DirectorySeparatorChar);
                path = Path.Combine(rootDirectory, path);
                path = Path.ChangeExtension(path, "cs");
                _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(path));

                using (var streamWriter = new StreamWriter(_fileSystem.File.Open(path, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite)))
                    streamWriter.Write(item.Value);
            }

            using (var stringWriter = new StreamWriter(_fileSystem.File.Open(projectFile, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite)))
            {
                stringWriter.WriteLine($"<Project Sdk=\"Microsoft.NET.Sdk\">");
                stringWriter.WriteLine($"  <PropertyGroup>");
                stringWriter.WriteLine($"    <TargetFramework>netcoreapp2.2</TargetFramework>");
                stringWriter.WriteLine($"  </PropertyGroup>");
                stringWriter.WriteLine($"  <ItemGroup>");
                stringWriter.WriteLine($"    <PackageReference Include=\"Avro\" Version=\"1.0.0\" />");
                stringWriter.WriteLine($"  </ItemGroup>");
                stringWriter.WriteLine($"</Project>");
            }
        }
    }
}
