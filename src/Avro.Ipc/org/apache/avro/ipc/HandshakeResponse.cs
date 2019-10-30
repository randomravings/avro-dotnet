using Avro;
using Avro.Schema;
using Avro.Types;
using System;
using System.Collections;
using System.Collections.Generic;

namespace org.apache.avro.ipc
{
    /// <summary></summary>
    [AvroNamedType("org.apache.avro.ipc", "HandshakeResponse")]
    public class HandshakeResponse : IAvroRecord
    {
        public static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema<RecordSchema>("{\"name\":\"org.apache.avro.ipc.HandshakeResponse\",\"type\":\"record\",\"fields\":[{\"name\":\"match\",\"type\":{\"name\":\"org.apache.avro.ipc.HandshakeMatch\",\"type\":\"enum\",\"symbols\":[\"BOTH\",\"CLIENT\",\"NONE\"]}},{\"name\":\"serverProtocol\",\"type\":[\"null\",\"string\"]},{\"name\":\"serverHash\",\"type\":[\"null\",{\"name\":\"org.apache.avro.ipc.MD5\",\"type\":\"fixed\",\"size\":16}]},{\"name\":\"meta\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"bytes\"}]}]}");
        public RecordSchema Schema => _SCHEMA;
        public int FieldCount => 4;
        /// <summary></summary>
        [AvroField("match")]
        public HandshakeMatch match
        {
            get;
            set;
        } = default(HandshakeMatch);

        /// <summary></summary>
        [AvroField("serverProtocol")]
        public string serverProtocol
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
                0 => match,
                1 => serverProtocol,
                2 => serverHash,
                3 => meta,
                _ => throw new IndexOutOfRangeException("Expected range: [0:3]."),
            };

            set
            {
                switch (i)
                {
                    case 0:
                        match = (HandshakeMatch)(value ?? default(HandshakeMatch));
                        break;
                    case 1:
                        serverProtocol = (string)(value ?? string.Empty);
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