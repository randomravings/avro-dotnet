using Avro;
using Avro.Schema;
using Avro.Types;
using System;

/// <summary></summary>
[AvroNamedType("", "Test_Name")]
public class Test_Name : IAvroError
{
    public static readonly ErrorSchema SCHEMA = AvroParser.ReadSchema<ErrorSchema>("{\"name\":\"Test_Name\",\"type\":\"error\",\"fields\":[]}");
    public ErrorSchema Schema => SCHEMA;
    public int FieldCount => 0;
    public object this[int i]
    {
        get
        {
            switch (i)
            {
                default:
                    throw new IndexOutOfRangeException("Expected range: [0:-1].");
            }
        }

        set
        {
            switch (i)
            {
                default:
                    throw new IndexOutOfRangeException("Expected range: [0:-1].");
            }
        }
    }
}