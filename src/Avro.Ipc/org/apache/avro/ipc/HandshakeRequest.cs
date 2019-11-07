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
        public static readonly RecordSchema SCHEMA = AvroParser.ReadSchema<RecordSchema>("{\"name\":\"org.apache.avro.ipc.HandshakeRequest\",\"type\":\"record\",\"fields\":[{\"name\":\"clientHash\",\"type\":{\"name\":\"org.apache.avro.ipc.MD5\",\"type\":\"fixed\",\"size\":16}},{\"name\":\"clientProtocol\",\"type\":[\"null\",\"string\"]},{\"name\":\"serverHash\",\"type\":\"org.apache.avro.ipc.MD5\"},{\"name\":\"meta\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"bytes\"}]}]}");
        public RecordSchema Schema => SCHEMA;
        public int FieldCount => 4;
        /// <summary></summary>
        [AvroField("clientHash")]
        public MD5 clientHash
        {
            get;
            set;
        } = new MD5();

        /// <summary></summary>
        [AvroField("clientProtocol")]
        public string clientProtocol
        {
            get;
            set;
        } = string.Empty;

        /// <summary></summary>
        [AvroField("serverHash")]
        public MD5 serverHash
        {
            get;
            set;
        } = new MD5();

        /// <summary></summary>
        [AvroField("meta")]
        public IDictionary<string, byte[]> meta
        {
            get;
            set;
        } = new Dictionary<string, byte[]>();

        public object? this[int i]
        {
            get => i switch
            {
                0 => clientHash,
                1 => clientProtocol,
                2 => serverHash,
                3 => meta,
                _ => throw new IndexOutOfRangeException("Expected range: [0:3]."),
            };

            set
            {
                switch (i)
                {
                    case 0:
                        clientHash = (MD5)(value ?? new MD5());
                        break;
                    case 1:
                        clientProtocol = (string)(value ?? string.Empty);
                        break;
                    case 2:
                        serverHash = (MD5)(value ?? new MD5());
                        break;
                    case 3:
                        meta = (IDictionary<string, byte[]>)(value ?? new Dictionary<string, byte[]>());
                        break;
                    default:
                        throw new IndexOutOfRangeException("Expected range: [0:3].");
                }
            }
        }
    }
}