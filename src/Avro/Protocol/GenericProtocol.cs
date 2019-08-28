using Avro.IO;
using Avro.Schema;
using Avro.Types;
using Avro.Utils;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Protocol
{
    public sealed class GenericProtocol : IProtocol
    {
        private static readonly object GUARD = new object();
        private static readonly BytesCompare COMPARE = new BytesCompare();
        private static readonly IDictionary<byte[], IDictionary<byte[], GenericProtocol>> PROTOCOL_PARIS = new Dictionary<byte[], IDictionary<byte[], GenericProtocol>>(COMPARE);

        private readonly IDictionary<string, IAvroReader<IAvroRecord>> _requestReaders = new Dictionary<string, IAvroReader<IAvroRecord>>();
        private readonly IDictionary<string, IAvroWriter<IAvroRecord>> _requestWriters = new Dictionary<string, IAvroWriter<IAvroRecord>>();
        private readonly IDictionary<string, IAvroReader<object>> _responseReaders = new Dictionary<string, IAvroReader<object>>();
        private readonly IDictionary<string, IAvroWriter<object>> _responseWriters = new Dictionary<string, IAvroWriter<object>>();
        private readonly IDictionary<string, IAvroReader<object>> _errorReaders = new Dictionary<string, IAvroReader<object>>();
        private readonly IDictionary<string, IAvroWriter<object>> _errorWrtiers = new Dictionary<string, IAvroWriter<object>>();

        public static GenericProtocol Get(AvroProtocol protocol, AvroProtocol remoteProtocol)
        {
            lock (GUARD)
            {
                if (!PROTOCOL_PARIS.TryGetValue(protocol.MD5, out var genericProtocolPairs))
                {
                    genericProtocolPairs = new Dictionary<byte[], GenericProtocol>(COMPARE);
                    PROTOCOL_PARIS.Add(protocol.MD5, genericProtocolPairs);
                }

                if (!genericProtocolPairs.TryGetValue(remoteProtocol.MD5, out var genericProtocolPair))
                {
                    genericProtocolPair = new GenericProtocol(protocol, remoteProtocol);
                    genericProtocolPairs.Add(remoteProtocol.MD5, genericProtocolPair);
                }
                return genericProtocolPair;
            }
        }

        public static bool AreSame(byte[] hash, byte[] remoteHash)
        {
            return COMPARE.Equals(hash, remoteHash);
        }

        public static bool Exists(byte[] hash, byte[] remoteHash)
        {
            lock (GUARD)
            {
                if (!PROTOCOL_PARIS.TryGetValue(hash, out var genericProtocolPairs))
                    return false;
                return genericProtocolPairs.ContainsKey(remoteHash);
            }
        }

        private GenericProtocol(AvroProtocol protocol, AvroProtocol remoteProtocol)
        {
            Protocol = protocol;
            RemoteProtocol = remoteProtocol;

            var messagePairs =
                from lm in protocol.Messages
                join rm in remoteProtocol.Messages on lm.Name equals rm.Name
                select new
                {
                    MessageName = lm.Name,
                    LocalMessage = lm,
                    RemoteMessage = rm
                };

            foreach (var messagePair in messagePairs)
            {
                var localRequestParameters =
                    from p in messagePair.LocalMessage.RequestParameters
                    join t in protocol.Types on p.Type equals t.FullName
                    select new RecordSchema.Field(p.Name, t)
                ;

                var remoteRequestParameters =
                    from p in messagePair.RemoteMessage.RequestParameters
                    join t in remoteProtocol.Types on p.Type equals t.FullName
                    select new RecordSchema.Field(p.Name, t)
                ;

                var localRequest = new RecordSchema($"{protocol.FullName}.messages.{messagePair.MessageName}", localRequestParameters);
                var remoteRequest = new RecordSchema($"{remoteProtocol.FullName}.messages.{messagePair.MessageName}", remoteRequestParameters);

                var requestReader = new DatumReader<IAvroRecord>(localRequest, remoteRequest);
                var requestWriter = new DatumWriter<IAvroRecord>(localRequest);

                _requestReaders.Add(messagePair.MessageName, requestReader);
                _requestWriters.Add(messagePair.MessageName, requestWriter);

                var responseReader = new DatumReader<object>(messagePair.LocalMessage.Response, messagePair.RemoteMessage.Response);
                var responseWriter = new DatumWriter<object>(messagePair.LocalMessage.Response);

                _responseReaders.Add(messagePair.MessageName, responseReader);
                _responseWriters.Add(messagePair.MessageName, responseWriter);

                var errorReader = new DatumReader<object>(messagePair.LocalMessage.Error, messagePair.RemoteMessage.Error);
                var errorWriter = new DatumWriter<object>(messagePair.LocalMessage.Error);

                _errorReaders.Add(messagePair.MessageName, errorReader);
                _errorWrtiers.Add(messagePair.MessageName, errorWriter);
            }
        }

        public AvroProtocol Protocol { get; private set; }
        public AvroProtocol RemoteProtocol { get; private set; }

        public T ReadRequest<T>(IAvroDecoder decoder, string message) where T : class, IAvroRecord
        {
            return _requestReaders[message].Read(decoder) as T;
        }

        public void WriteRequest<T>(IAvroEncoder encoder, string message, T record) where T : class, IAvroRecord
        {
            _requestWriters[message].Write(encoder, record);
        }

        public T ReadResponse<T>(IAvroDecoder decoder, string message) where T : class
        {
            return _responseReaders[message].Read(decoder) as T;
        }

        public void WriteReponse<T>(IAvroEncoder encoder, string message, T response) where T : class
        {
            _responseWriters[message].Write(encoder, response);
        }

        public T ReadError<T>(IAvroDecoder decoder, string message) where T : class
        {
            return _errorReaders[message].Read(decoder) as T;
        }

        public void WriteError<T>(IAvroEncoder encoder, string message, T error) where T : class
        {
            _errorWrtiers[message].Write(encoder, error);
        }
    }
}
