using System;
using System.Collections.Immutable;

namespace Avro.Utils
{
    public static class Constants
    {
        public static readonly DateTime UNIX_EPOCH = new DateTime(1970, 1, 1);
        public static ImmutableHashSet<string> RESERVED_TAGS = ImmutableHashSet.Create(
            "type",
            "name",
            "namespace",
            "logicalType",
            "doc",

            "null",
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "string",
            "bytes",
            "array",
            "map",
            "fixed",
            "enum",
            "record",
            "error",
            "protocol",
            "union",
            "date",
            "time-millis",
            "time-micros",
            "timestamp-millis",
            "timestamp-micros",
            "duration",

            "items",
            "values",
            "fields",
            "size",
            "default",
            "scale",
            "precision",
            "types",
            "messages",
            "request",
            "response ",
            "errors "
        );
    }
}
