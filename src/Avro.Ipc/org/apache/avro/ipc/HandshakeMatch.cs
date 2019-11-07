#pragma warning disable CS8600, CS8601, CS8618 // Nullability warnings.

#pragma warning disable IDE1006, IDE0066 // Style warnings.

using Avro;
using Avro.Schema;
using Avro.Types;
using System;
using System.Collections;
using System.Collections.Generic;

namespace org.apache.avro.ipc
{
    /// <summary></summary>
    [AvroType("org.apache.avro.ipc", "HandshakeMatch")]
    public enum HandshakeMatch
    {
        BOTH,
        CLIENT,
        NONE
    }
}