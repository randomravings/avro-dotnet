namespace Avro.Types
{
    public interface IAvroProtocol
    {
        AvroProtocol Protocol { get; }
        void Request(ICallbackRequestor requestor, string messageName, object[] args, object callback);
    }

    public interface ICallbackRequestor
    {
        void Request<T>(string messageName, object[] args, object callback);
    }
}
