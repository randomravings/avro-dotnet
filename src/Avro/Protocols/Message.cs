using Avro.Schemas;
using System.Collections.Generic;

namespace Avro.Protocols
{
    public class Message
    {
        private readonly IList<RecordSchema> _requests;

        /// <summary>
        /// Name of the message
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Documentation for the message
        /// </summary>
        public string Doc { get; set; }

        /// <summary>
        /// Anonymous record for the list of parameters for the request fields
        /// </summary>
        public IList<RecordSchema> Request { get; set; }

        /// <summary>
        /// Schema object for the 'response' attribute
        /// </summary>
        public Schema Response { get; set; }

        /// <summary>
        /// Union schema object for the 'error' attribute
        /// </summary>
        public UnionSchema Error { get; set; }

        /// <summary>
        /// Optional one-way attribute
        /// </summary>
        public bool? Oneway { get; set; }

        /// <summary>
        /// Explicitly defined protocol errors plus system added "string" error
        /// </summary>
        public UnionSchema SupportedErrors { get; set; }

        public Message()
        {
            _requests = new List<RecordSchema>();
        }

        public Message(string name)
        {
            Name = name;
            _requests = new List<RecordSchema>();
        }

        public override string ToString() => Name;

        /// <summary>
        /// Tests equality of this Message object with the passed object
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj == this) return true;
            if (!(obj is Message)) return false;

            var that = obj as Message;
            return Name.Equals(that.Name) &&
                   Request.Equals(that.Request) &&
                   AreEqual(Response, that.Response) &&
                   AreEqual(Error, that.Error);
        }

        /// <summary>
        /// Returns the hash code of this Message object
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return Name.GetHashCode() +
                   Request.GetHashCode() +
                  (Response == null ? 0 : Response.GetHashCode()) +
                  (Error == null ? 0 : Error.GetHashCode());
        }

        /// <summary>
        /// Tests equality of two objects taking null values into account
        /// </summary>
        /// <param name="o1"></param>
        /// <param name="o2"></param>
        /// <returns></returns>
        protected static bool AreEqual(object o1, object o2)
        {
            return o1 == null ? o2 == null : o1.Equals(o2);
        }
    }
}
