using Avro;
using Avro.Monad;
using System.Linq;

namespace DebugConsole
{
    class Program
    {
        private static readonly AvroProtocol HELLO_PROTOCOL = AvroParser.ReadProtocol(@"
        {
          ""namespace"": ""com.acme"",
          ""protocol"": ""HelloWorld"",
          ""doc"": ""Protocol Greetings"",

          ""types"": [
            {""name"": ""Greeting"", ""type"": ""record"", ""fields"": [
              {""name"": ""message"", ""type"": ""string""}]},
            {""name"": ""Curse"", ""type"": ""error"", ""fields"": [
              {""name"": ""message"", ""type"": ""string""}]}
          ],

          ""messages"": {
            ""hello"": {
              ""doc"": ""Say hello."",
              ""request"": [{""name"": ""greeting"", ""type"": ""Greeting"" }],
              ""response"": ""Greeting"",
              ""errors"": [""Curse""]
            }
          }
        }");

        static void Main()
        {

            var result = from a in Maybe<int>.Some(4)
                         from b in Maybe<int>.Some(6)
                         select a + b;

            var foo = 
                Maybe<int>.Some(42)
                .Bind<bool>(r => (r % 2) == 0);
            foo.GetOrElse(r => r.ToString(), string.Empty);


            //var foo = new JsonEncoderTest();
            //foo.TestSimple();
        }
    }
}
