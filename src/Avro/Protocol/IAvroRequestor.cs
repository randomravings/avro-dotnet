using Avro.IO;
using Avro.Protocol.Schema;
using Avro.Types;

namespace Avro.Protocol
{
    public interface IAvroRequestor
    {
        AvroProtocol Local { get; }
        AvroProtocol Remote { get; }
        void WriteRequest<T>(IAvroEncoder encoder, string message, T record) where T : notnull, IAvroRecord;
        T ReadResponse<T>(IAvroDecoder decoder, string message)
            where T : notnull
        ;
        U ReadError<U, T1>(IAvroDecoder decoder, string message)
            where T1 : notnull
            where U : notnull, AvroUnion<string, T1>
        ;
        U ReadError<U, T1, T2>(IAvroDecoder decoder, string message)
            where T1 : notnull
            where T2 : notnull
            where U : notnull, AvroUnion<string, T1, T2>
        ;
        U ReadError<U, T1, T2, T3>(IAvroDecoder decoder, string message)
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
            where U : notnull, AvroUnion<string, T1, T2, T3>
        ;
        U ReadError<U, T1, T2, T3, T4>(IAvroDecoder decoder, string message)
            where T1 : notnull
            where T2 : notnull
            where T3 : notnull
            where T4 : notnull
            where U : notnull, AvroUnion<string, T1, T2, T3, T4>
        ;
    }
}
