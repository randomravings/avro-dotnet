using Avro.Schema;
using System;
using System.Collections.Generic;
using System.Text;

namespace Avro.Container
{
    public class Metadata : Dictionary<string, byte[]>
    {
        public AvroSchema Schema
        {
            get
            {
                if (!TryGetValue("avro.schema", out var bytes))
                    return EmptySchema.Value;
                return AvroParser.ReadSchema(Encoding.UTF8.GetString(bytes));
            }
            set
            {
                var bytes = Encoding.UTF8.GetBytes(value.ToAvroCanonical());
                if (ContainsKey("avro.schema"))
                    this["avro.schema"] = bytes;
                else
                    Add("avro.schema", bytes);
            }
        }

        public Codec Codec
        {
            get
            {
                if (!TryGetValue("avro.codec", out var codecBytes))
                    return Codec.Null;
                return Enum.Parse<Codec>(Encoding.UTF8.GetString(codecBytes), true);
            }
            set
            {
                var bytes = Encoding.UTF8.GetBytes(value.ToString());
                if (ContainsKey("avro.codec"))
                    this["avro.codec"] = bytes;
                else
                    Add("avro.codec", bytes);
            }
        }
    }
}
