using System;

namespace Avro.Specific
{
    public abstract class AvroRemoteException : AvroException
    {
        public AvroRemoteException()
            : base() { }
        public AvroRemoteException(string message)
            : base(message) { }
        public AvroRemoteException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}
