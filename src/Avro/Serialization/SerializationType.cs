using System;

namespace Avro.Serialization
{
    /// <summary>
    /// Attribute to decorate <see cref="AvroSchema"/>.
    /// It is used to match compatibility with system or user defined types during
    /// serialization/deserialization.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
    public class SerializationType : Attribute
    {
        public SerializationType(Type defaultType)
        {
            DefaultType = defaultType;
        }
        /// <summary>
        /// Default type.
        /// null means there is no default instantiation performed and requires
        /// that a type is selected explicitly and that matches a type defined in
        /// <see cref="CompatibleTypes"/>.
        /// </summary>
        public Type DefaultType { get; private set; }
        /// <summary>
        /// List of compatbile types that are matched explicit type is selected.
        /// </summary>
        public Type[] CompatibleTypes { get; set; } = new Type[0];
        /// <summary>
        /// List of reserved generic arguments.
        /// The length does not have to match the list of generic arguments of the type.
        /// A null is ignored and ca be used to pad the preceding arguments.
        /// Eg:
        /// - IDictionary<string,*> can be reserved by new Type { typeof(string) }
        /// - Typle<*,*,int,*> can be reserved by new Type { null, null, typeof(int) }
        /// </summary>
        public Type[] ReservedGenericArguments { get; set; } = new Type[0];
    }
}
