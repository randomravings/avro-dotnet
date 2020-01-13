#pragma warning disable CS8600, CS8601, CS8618 // Nullability warnings.

#pragma warning disable IDE1006, IDE0066 // Style warnings.

using Avro;
using Avro.Schema;
using Avro.Types;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace org.apache.avro.ipc
{
    /// <summary></summary>
    [DataContract(Name = "HandshakeMatch", Namespace = "org.apache.avro.ipc")]
    public enum HandshakeMatch
    {
        [EnumMember(Value = "BOTH")]
        BOTH,
        [EnumMember(Value = "CLIENT")]
        CLIENT,
        [EnumMember(Value = "NONE")]
        NONE
    }
}