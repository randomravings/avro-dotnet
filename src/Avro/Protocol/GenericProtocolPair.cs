using Avro.IO;
using Avro.Protocol.Schema;
using Avro.Schema;
using Avro.Types;
using Avro.Utils;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Protocol
{
    public sealed class GenericProtocolPair
    {
        private static readonly object GUARD = new object();
        private static readonly BytesCompare COMPARE = new BytesCompare();
        private static readonly IDictionary<byte[], IDictionary<byte[], GenericProtocolPair>> CACHE = new Dictionary<byte[], IDictionary<byte[], GenericProtocolPair>>();
        public static GenericProtocolPair Get(AvroProtocol local, AvroProtocol remote)
        {
            lock (GUARD)
            {
                if (!CACHE.TryGetValue(local.MD5, out var genericProtocolPairs))
                {
                    genericProtocolPairs = new Dictionary<byte[], GenericProtocolPair>(COMPARE);
                    CACHE.Add(local.MD5, genericProtocolPairs);
                }

                if (!genericProtocolPairs.TryGetValue(remote.MD5, out var genericProtocolPair))
                {
                    genericProtocolPair = new GenericProtocolPair(local, remote);
                    genericProtocolPairs.Add(remote.MD5, genericProtocolPair);
                }
                return genericProtocolPair;
            }
        }

        public static bool AreSame(byte[] x, byte[] y)
        {
            return COMPARE.Equals(x, y);
        }

        public static bool Exists(byte[] x, byte[] y)
        {
            if (!CACHE.TryGetValue(x, out var z))
                return false;
            return z.ContainsKey(y);
        }

        private GenericProtocolPair(AvroProtocol local, AvroProtocol remote)
        {
            LocalHash = local.MD5;
            RemoteHash = remote.MD5;

            var requestReaders = new Dictionary<string, IAvroReader<GenericRecord>>();
            var requestWriters = new Dictionary<string, IAvroWriter<GenericRecord>>();
            var responseReaders = new Dictionary<string, IAvroReader<object>>();
            var responseWriters = new Dictionary<string, IAvroWriter<object>>();
            var errorReaders = new Dictionary<string, IAvroReader<object>>();
            var errorWriters = new Dictionary<string, IAvroWriter<object>>();

            var messagePairs =
                from lm in local.Messages
                join rm in remote.Messages on lm.Name equals rm.Name
                select (lm.Name, lm, rm)
            ;

            foreach ((var messageName, var localMessage, var remoteMessage) in messagePairs)
            {
                var localRequestParameters =
                    from p in localMessage.RequestParameters
                    join t in local.Types on p.Type.FullName equals t.FullName
                    select new RecordFieldSchema(p.Name, t)
                ;

                var remoteRequestParameters =
                    from p in remoteMessage.RequestParameters
                    join t in remote.Types on p.Type.FullName equals t.FullName
                    select new RecordFieldSchema(p.Name, t)
                ;

                var localRequest = new RecordSchema($"{local.FullName}.messages.{messageName}", localRequestParameters);
                var remoteRequest = new RecordSchema($"{remote.FullName}.messages.{messageName}", remoteRequestParameters);

                var requestReader = new DatumReader<GenericRecord>(localRequest, remoteRequest);
                var requestWriter = new DatumWriter<GenericRecord>(localRequest);

                requestReaders.Add(messageName, requestReader);
                requestWriters.Add(messageName, requestWriter);

                var responseReader = new DatumReader<object>(localMessage.Response, remoteMessage.Response);
                var responseWriter = new DatumWriter<object>(localMessage.Response);

                responseReaders.Add(messageName, responseReader);
                responseWriters.Add(messageName, responseWriter);

                var errorReader = new DatumReader<object>(localMessage.Error, remoteMessage.Error);
                var errorWriter = new DatumWriter<object>(localMessage.Error);

                errorReaders.Add(messageName, errorReader);
                errorWriters.Add(messageName, errorWriter);
            }

            RequestReaders = requestReaders;
            RequestWriters = requestWriters;
            ResponseReaders = responseReaders;
            ResponseWriters = responseWriters;
            ErrorReaders = errorReaders;
            ErrorWriters = errorWriters;
        }

        public byte[] LocalHash { get; private set; }
        public byte[] RemoteHash { get; private set; }
        public IReadOnlyDictionary<string, IAvroReader<GenericRecord>> RequestReaders { get; private set; }
        public IReadOnlyDictionary<string, IAvroWriter<GenericRecord>> RequestWriters { get; private set; }
        public Dictionary<string, IAvroReader<object>> ResponseReaders { get; private set; }
        public Dictionary<string, IAvroWriter<object>> ResponseWriters { get; private set; }
        public Dictionary<string, IAvroReader<object>> ErrorReaders { get; private set; }
        public Dictionary<string, IAvroWriter<object>> ErrorWriters { get; private set; }
    }
}
