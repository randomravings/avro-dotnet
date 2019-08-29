using Avro;
using Avro.Schema;
using Avro.Types;
using System;
using System.Collections;
using System.Collections.Generic;

namespace org.apache.avro.ipc
{
    /// <summary></summary>
    public enum HandshakeMatch
    {
        BOTH,
        CLIENT,
        NONE
    }
}