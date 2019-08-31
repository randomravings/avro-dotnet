using Avro;
using Avro.Schema;
using Avro.Types;
using System;
using System.Collections;
using System.Collections.Generic;

namespace org.apache.avro.ipc
{
    /// <summary></summary>
    [AvroNamedType("org.apache.avro.ipc", "HandshakeMatch")]
    public enum HandshakeMatch
    {
        BOTH,
        CLIENT,
        NONE
    }
}