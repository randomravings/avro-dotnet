using Avro;
using Avro.Specific;
using System;
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