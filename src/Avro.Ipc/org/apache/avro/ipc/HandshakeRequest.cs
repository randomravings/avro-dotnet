using Avro;
using Avro.Specific;
using System;
using System.Collections.Generic;

#pragma warning disable IDE1006 // Naming Styles
namespace org.apache.avro.ipc
{
    /// <summary></summary>
    public class HandshakeRequest : Avro.Specific.ISpecificRecord
    {
        public static readonly Avro.Schema _SCHEMA = Avro.Schema.Parse("{\"name\":\"org.apache.avro.ipc.HandshakeRequest\",\"type\":\"record\",\"fields\":[{\"name\":\"clientHash\",\"type\":{\"name\":\"org.apache.avro.ipc.MD5\",\"type\":\"fixed\",\"size\":16}},{\"name\":\"clientProtocol\",\"type\":[\"null\",\"string\"]},{\"name\":\"serverHash\",\"type\":\"org.apache.avro.ipc.MD5\"},{\"name\":\"meta\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"bytes\"}]}]}");
        public Avro.Schema Schema => _SCHEMA;
        public int FieldCount => 4;
        /// <summary></summary>
        public org.apache.avro.ipc.MD5 clientHash
        {
            get;
            set;
        }

        /// <summary></summary>
        public object clientProtocol
        {
            get;
            set;
        }

        /// <summary></summary>
        public org.apache.avro.ipc.MD5 serverHash
        {
            get;
            set;
        }

        /// <summary></summary>
        public object meta
        {
            get;
            set;
        }

        public object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0:
                    return clientHash;
                case 1:
                    return clientProtocol;
                case 2:
                    return serverHash;
                case 3:
                    return meta;
                default:
                    throw new IndexOutOfRangeException("Expected range: [0:3].");
            }
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    clientHash = (org.apache.avro.ipc.MD5)fieldValue;
                    break;
                case 1:
                    clientProtocol = (object)fieldValue;
                    break;
                case 2:
                    serverHash = (org.apache.avro.ipc.MD5)fieldValue;
                    break;
                case 3:
                    meta = (object)fieldValue;
                    break;
                default:
                    throw new IndexOutOfRangeException("Expected range: [0:3].");
            }
        }
    }
}
#pragma warning restore IDE1006 // Naming Styles