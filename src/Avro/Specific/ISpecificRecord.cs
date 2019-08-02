namespace Avro.Specific
{
    public interface ISpecificRecord
    {
        Schema Schema { get; }
        int FieldCount { get; }
        object Get(int fieldPos);
        void Put(int fieldPos, object fieldValue);
    }
}
