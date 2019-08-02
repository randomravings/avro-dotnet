using Avro;
using Avro.Utils;
using NUnit.Framework;

namespace Avro.Test.Utils
{
    [TestFixture]
    public class NameValidatorTest
    {
        [TestCase]
        public void TestName()
        {
            Assert.DoesNotThrow(
                () => NameValidator.ValidateName("abc")
            );

            Assert.DoesNotThrow(
                () => NameValidator.ValidateName("abc123")
            );

            Assert.Throws(
                typeof(AvroParseException),
                () => NameValidator.ValidateName("a.bc")
            );

            Assert.Throws(
                typeof(AvroParseException),
                () => NameValidator.ValidateName("a@bc")
            );
        }

        [TestCase]
        public void TestNamespace()
        {
            Assert.Throws(
                typeof(AvroParseException),
                () => NameValidator.ValidateNamespace(string.Empty)
            );

            Assert.DoesNotThrow(
                () => NameValidator.ValidateNamespace(null)
            );

            Assert.DoesNotThrow(
                () => NameValidator.ValidateNamespace("abc")
            );

            Assert.DoesNotThrow(
                () => NameValidator.ValidateNamespace("a.bc")
            );
        }

        [TestCase]
        public void TestSymbols()
        {
            Assert.DoesNotThrow(
                () => NameValidator.ValidateSymbols(new string[] { "A", "B", "C" })
            );

            Assert.Throws(
                typeof(AvroParseException),
                () => NameValidator.ValidateSymbols(new string[] { "A", "B", "A" })
            );
        }
    }
}
