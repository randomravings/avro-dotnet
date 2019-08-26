using Avro.Schemas;
using System.Collections.Generic;

namespace Avro.IO.Formatting
{
    public class JsonItemDescriptor
    {
        public JsonItemDescriptor(string propertyName, int limit, bool isRecord)
        {
            PropertyName = propertyName;
            Limit = limit;
            IsRecord = isRecord;
        }
        public string PropertyName { get; set; }
        public int Limit { get; set; }
        public bool IsRecord { get; set; }
        public override string ToString()
        {
            return $"{PropertyName} -> {Limit}:{IsRecord}";
        }

        public static void AddHeading(AvroSchema schema, IList<JsonItemDescriptor> headings)
        {
            switch (schema)
            {
                case NullSchema _:
                    headings.Add(new JsonItemDescriptor("null", headings.Count, false));
                    break;
                case BooleanSchema _:
                    headings.Add(new JsonItemDescriptor("boolean", headings.Count, false));
                    break;
                case IntSchema _:
                    headings.Add(new JsonItemDescriptor("int", headings.Count, false));
                    break;
                case LongSchema _:
                    headings.Add(new JsonItemDescriptor("long", headings.Count, false));
                    break;
                case FloatSchema _:
                    headings.Add(new JsonItemDescriptor("float", headings.Count, false));
                    break;
                case DoubleSchema _:
                    headings.Add(new JsonItemDescriptor("double", headings.Count, false));
                    break;
                case StringSchema _:
                    headings.Add(new JsonItemDescriptor("string", headings.Count, false));
                    break;
                case BytesSchema _:
                    headings.Add(new JsonItemDescriptor("bytes", headings.Count, false));
                    break;
                case DateSchema _:
                    headings.Add(new JsonItemDescriptor("date", headings.Count, false));
                    break;
                case DecimalSchema _:
                    headings.Add(new JsonItemDescriptor("decimal", headings.Count, false));
                    break;
                case UuidSchema _:
                    headings.Add(new JsonItemDescriptor("uuid", headings.Count, false));
                    break;
                case TimeMillisSchema _:
                    headings.Add(new JsonItemDescriptor("time-millis", headings.Count, false));
                    break;
                case TimeMicrosSchema _:
                    headings.Add(new JsonItemDescriptor("time-millis", headings.Count, false));
                    break;
                case TimeNanosSchema _:
                    headings.Add(new JsonItemDescriptor("time-nanos", headings.Count, false));
                    break;
                case TimestampMillisSchema _:
                    headings.Add(new JsonItemDescriptor("timestamp-millis", headings.Count, false));
                    break;
                case TimestampMicrosSchema _:
                    headings.Add(new JsonItemDescriptor("timestamp-millis", headings.Count, false));
                    break;
                case TimestampNanosSchema _:
                    headings.Add(new JsonItemDescriptor("timestamp-nanos", headings.Count, false));
                    break;
                case DurationSchema _:
                    headings.Add(new JsonItemDescriptor("duration", headings.Count, false));
                    break;
                case ArraySchema a:
                    var arrayHeading = new JsonItemDescriptor("array", 0, false);
                    headings.Add(arrayHeading);
                    AddHeading(a.Items, headings);
                    arrayHeading.Limit = headings.Count;
                    break;
                case MapSchema m:
                    var mapHeading = new JsonItemDescriptor("map", 0, false);
                    headings.Add(mapHeading);
                    AddHeading(m.Values, headings);
                    mapHeading.Limit = headings.Count;
                    break;
                case EnumSchema e:
                    headings.Add(new JsonItemDescriptor(e.FullName, headings.Count, false));
                    break;
                case FixedSchema e:
                    headings.Add(new JsonItemDescriptor(e.FullName, headings.Count, false));
                    break;
                case RecordSchema r:
                    var recordHeading = new JsonItemDescriptor(r.FullName, 0, true);
                    headings.Add(recordHeading);
                    foreach (var f in r)
                        AddHeading(f.Type, headings);
                    recordHeading.Limit = headings.Count;
                    break;
            }
        }
    }
}
