using Avro;
using Avro.Schema;
using Avro.Types;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Test.Avro.Perf
{
    /// <summary></summary>
    [AvroNamedType("Test.Avro.Perf", "TestRecord")]
    public class TestRecord : IAvroRecord
    {
        public static readonly RecordSchema _SCHEMA = AvroParser.ReadSchema<RecordSchema>("{\"name\":\"Test.Avro.Perf.TestRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"Bool\",\"type\":\"boolean\"},{\"name\":\"Int32\",\"type\":\"int\"},{\"name\":\"Int64\",\"type\":\"long\"},{\"name\":\"Single\",\"type\":\"float\"},{\"name\":\"Double\",\"type\":\"double\"},{\"name\":\"String\",\"type\":\"string\"}]}");
        public RecordSchema Schema => _SCHEMA;
        public int FieldCount => 6;
        /// <summary></summary>
        [AvroField("Bool")]
        public bool Bool
        {
            get;
            set;
        }

        /// <summary></summary>
        [AvroField("Int32")]
        public int Int32
        {
            get;
            set;
        }

        /// <summary></summary>
        [AvroField("Int64")]
        public long Int64
        {
            get;
            set;
        }

        /// <summary></summary>
        [AvroField("Single")]
        public float Single
        {
            get;
            set;
        }

        /// <summary></summary>
        [AvroField("Double")]
        public double Double
        {
            get;
            set;
        }

        /// <summary></summary>
        [AvroField("String")]
        public string String
        {
            get;
            set;
        } = string.Empty;

        public object? this[int i]
        {
            get => i switch
            {
                0 => Bool,
                1 => Int32,
                2 => Int64,
                3 => Single,
                4 => Double,
                5 => String,
                _ => throw new IndexOutOfRangeException("Expected range: [0:5]."),
            };
            set
            {
                switch (i)
                {
                    case 0:
                        Bool = (bool)(value ?? 0);
                        break;
                    case 1:
                        Int32 = (int)(value ?? 0);
                        break;
                    case 2:
                        Int64 = (long)(value ?? 0);
                        break;
                    case 3:
                        Single = (float)(value ?? 0);
                        break;
                    case 4:
                        Double = (double)(value ?? 0);
                        break;
                    case 5:
                        String = (string)(value ?? string.Empty);
                        break;
                    default:
                        throw new IndexOutOfRangeException("Expected range: [0:5].");
                }
            }
        }
    }
}