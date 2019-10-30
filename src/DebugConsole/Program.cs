using Avro.Schema;
using System;
using Test.Avro.IO;
using Test.Avro.Resolution;

namespace DebugConsole
{
    class Program
    {
        static void Main()
        {
            //var foo = new JsonEncoderTest();
            //foo.TestSimple();
            var foo = new ReadWriteAdvTest();
            foo.RecordGenericTest();
        }
    }
}
