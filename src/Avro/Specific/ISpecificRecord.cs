namespace Avro.Specific
{
    /// <summary>
    /// Interface class for generated classes
    /// </summary>
    public interface ISpecificRecord
    {
        Schema Schema { get; }
        int FieldCount { get; }
        object Get(int fieldPos);
        void Put(int fieldPos, object fieldValue);
    }
}
