using Avro;
using Avro.Specific;
using System;
using System.Collections.Generic;

namespace org.apache.avro.ipc
{
    /// <summary></summary>
    public class MD5 : Avro.Specific.ISpecificFixed
    {
        public static readonly Avro.Schema _SCHEMA = Avro.Schema.Parse("{\"name\":\"org.apache.avro.ipc.MD5\",\"type\":\"fixed\",\"size\":16}");
        public const int _SIZE = 16;
        public MD5()
        {
            Value = new byte[_SIZE];
        }

        public Avro.Schema Schema => _SCHEMA;
        public int FixedSize => _SIZE;
        public byte[] Value
        {
            get;
            private set;
        }
    }
}