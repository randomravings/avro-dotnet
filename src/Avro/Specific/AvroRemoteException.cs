namespace Avro.Specific
{
    public abstract class AvroRemoteException : AvroException
    {
        public AvroRemoteException(string message)
            : base(message) { }
    }
}
