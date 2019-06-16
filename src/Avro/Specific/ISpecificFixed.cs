namespace Avro.Specific
{
    public interface ISpecificFixed
    {
        Schema Schema { get; }
        int FixedSize { get; }
        byte[] Value { get; }
    }
}
