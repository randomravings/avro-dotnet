using System;

namespace Avro.Specific
{
    /// <summary>
    /// Interface class for generated classes
    /// </summary>
    public interface ISpecificEnum<T> where T : struct, Enum
    {
        Schema Schema { get; }
        int FieldCount { get; }
        object Get(int fieldPos);
        void Put(int fieldPos, object fieldValue);
    }
}
