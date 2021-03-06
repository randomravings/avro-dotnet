using Avro;
using Avro.Schema;
using Avro.Types;
using System;
using System.Collections;
using System.Collections.Generic;

namespace org.apache.avro.ipc
{
    /// <summary></summary>
    [AvroNamedType("org.apache.avro.ipc", "HandshakeRequest")]
    public class HandshakeRequest : IAvroRecord
    {
        public static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema<RecordSchema>("{\"name\":\"org.apache.avro.ipc.HandshakeRequest\",\"type\":\"record\",\"fields\":[{\"name\":\"clientHash\",\"type\":{\"name\":\"org.apache.avro.ipc.MD5\",\"type\":\"fixed\",\"size\":16}},{\"name\":\"clientProtocol\",\"type\":[\"null\",\"string\"]},{\"name\":\"serverHash\",\"type\":\"org.apache.avro.ipc.MD5\"},{\"name\":\"meta\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"bytes\"}]}]}");
        public RecordSchema Schema => _SCHEMA;
        public int FieldCount => 4;
        /// <summary></summary>
        [AvroField("clientHash")]
        public MD5 clientHash
        {
            get;
            set;
        }

        /// <summary></summary>
        [AvroField("clientProtocol")]
        public string clientProtocol
        {
            get;
            set;
        }

        /// <summary></summary>
        [AvroField("serverHash")]
        public MD5 serverHash
        {
            get;
            set;
        }

        /// <summary></summary>
        [AvroField("meta")]
        public IDictionary<string, byte[]> meta
        {
            get;
            set;
        }

        public object this[int i]
        {
            get
            {
                switch (i)
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

            set
            {
                switch (i)
                {
                    case 0:
                        clientHash = (MD5)value;
                        break;
                    case 1:
                        clientProtocol = (string)value;
                        break;
                    case 2:
                        serverHash = (MD5)value;
                        break;
                    case 3:
                        meta = (IDictionary<string, byte[]>)value;
                        break;
                    default:
                        throw new IndexOutOfRangeException("Expected range: [0:3].");
                }
            }
        }
    }
}