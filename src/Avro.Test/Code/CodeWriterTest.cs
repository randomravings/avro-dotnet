using Avro.Code;
using Avro.Schema;
using NUnit.Framework;
using System.IO;
using System.IO.Abstractions.TestingHelpers;
using System.Linq;

namespace Avro.Test.Code
{
    [TestFixture]
    public class CodeWriterTest
    {
        [TestCase]
        public void ProjectTest()
        {
            Assert.NotNull(new CodeWriter());

            var schema01 = new EnumSchema("TestEnum", "NS.Stuff", new string[] { "A", "B", "C" });
            var schema02 = new FixedSchema("TestFixed", "NS.Stuff", 14);
            var schema03 = new RecordSchema(
                "TestRecord",
                "NS.Stuff", new RecordSchema.Field[]
                {
                    new RecordSchema.Field("FieldA", new IntSchema()),
                    new RecordSchema.Field("FieldB", schema01),
                    new RecordSchema.Field("FieldC", schema02),
                }
            );

            var codeGen = new CodeGen();
            codeGen.AddSchema(schema01);
            codeGen.AddSchema(schema02);
            codeGen.AddSchema(schema03);

            var projectname = "test_project";

            var mockFileSystem = new MockFileSystem();
            mockFileSystem.AddDirectory(projectname);
            var rootDir = mockFileSystem.AllDirectories.First(r => r.EndsWith(projectname));

            var codeWriter = new CodeWriter(mockFileSystem);
            codeWriter.WriteProject(rootDir, codeGen, projectname);

            var class01 = GetCodeFileName(schema01, rootDir);
            var class02 = GetCodeFileName(schema02, rootDir);
            var class03 = GetCodeFileName(schema03, rootDir);

            Assert.IsTrue(mockFileSystem.FileExists(class01));
            Assert.IsTrue(mockFileSystem.FileExists(class02));
            Assert.IsTrue(mockFileSystem.FileExists(class03));

            var projectFile = Path.ChangeExtension(Path.Combine(rootDir, projectname), ".csproj");
            Assert.IsTrue(mockFileSystem.FileExists(projectFile));
        }

        private static string GetCodeFileName(NamedSchema schema, string root)
        {
            var relativePath = Path.ChangeExtension(Path.Combine(schema.FullName.Split('.')), ".cs");
            var canonicalPath = Path.Combine(root, relativePath);
            return canonicalPath;
        }
    }
}
