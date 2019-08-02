namespace Avro.Specific
{
    public interface ISpecificProtocol
    {
        Protocol Protocol { get; }
        void Request(ICallbackRequestor requestor, string messageName, object[] args, object callback);
    }

    public interface ICallbackRequestor
    {
        void Request<T>(string messageName, object[] args, object callback);
    }
}
