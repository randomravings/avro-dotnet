using Avro.Schema;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Avro.Types
{
    public abstract class AvroUnion : IEquatable<AvroUnion>
    {
        protected int _index = 0;
        protected Type _type = typeof(AvroNull);
        protected object _value = AvroNull.Value;

        public Type Type => _type;
        public long Index => _index;
        public object Value => _value;
        public override string ToString() => _value.ToString();
        public override bool Equals(object obj) => _value.Equals(obj);
        public bool Equals(AvroUnion other) => EqualityComparer<object>.Default.Equals(Value, other.Value);
        public override int GetHashCode() => HashCode.Combine(Value);
        protected T Get<T>(int index) where T : notnull
        {
            if (_index != index)
                throw new InvalidCastException($"Union is instance of '{_type.FullName}'");
            return (T)_value;
        }
        protected void Set<T>(int index, T value) where T : notnull
        {
            _index = index;
            _type = typeof(T);
            _value = value;
        }
        public static bool operator ==(AvroUnion left, AvroUnion right) => EqualityComparer<AvroUnion>.Default.Equals(left, right);
        public static bool operator !=(AvroUnion left, AvroUnion right) => !(left == right);
    }

    public abstract class GenericUnion<TImpl> : AvroUnion where TImpl : GenericUnion<TImpl>
    {
        private readonly object _default;
        protected GenericUnion()
        {
            _index = -1;
            _type = typeof(object);
            _value = new object();
            _default = new object();
            Schema = new UnionSchema();
            Types = new Type[0];
        }
        public GenericUnion(UnionSchema schema, Type[] types, object defaultValue)
        {
            Schema = schema;
            Types = types;
            _default = defaultValue;
            if (schema.Count != types.Length)
                throw new ArgumentException("Schema and Types count mismatch");
            if (!IsValid(this, _index, _default))
                throw new ArgumentException("Default Type mismatch");
            SetDefault();
        }
        public GenericUnion(TImpl model)
        {
            _default = model._default;
            Schema = model.Schema;
            Types = Types;
            SetDefault();
        }
        public UnionSchema Schema { get; private set; }
        public Type[] Types { get; private set; }
        public void SetValue<T>(int index, T value) where T : notnull
        {
            if (!IsValid(this, index, value))
                throw new ArgumentException("Type mismatch");
            Set(index, value);
        }
        public T GetValue<T>() where T : notnull
        {
            if (!IsValid(this, _index, _value))
                throw new ArgumentException("Type mismatch");
            return Get<T>(_index);
        }
        public void SetDefault()
        {
            _index = 0;
            _type = Types[0];
            _value = _default;
        }
        public static bool IsValid(GenericUnion<TImpl> union, int index, object value) =>
            value.GetType().Equals(union.Types[index]) && value switch
            {
                GenericEnum e => e.Schema.Equals(union.Schema[index]),
                GenericError e => e.Schema.Equals(union.Schema[index]),
                GenericFixed e => e.Schema.Equals(union.Schema[index]),
                GenericRecord e => e.Schema.Equals(union.Schema[index]),
                _ => true
            };

        protected abstract TImpl New();
    }

    public class AvroUnion<T1> :
        AvroUnion
        where T1 : notnull
    {
        protected AvroUnion() { }
        public AvroUnion(T1 value) => SetT1(value);
        public T1 GetT1() => Get<T1>(0);
        public void SetT1(T1 value) => Set(0, value);
        public static implicit operator T1(AvroUnion<T1> union) => union.GetT1();
        public static implicit operator AvroUnion<T1>(T1 value) => new AvroUnion<T1>(value);
    }

    public class AvroUnion<T1, T2> :
        AvroUnion<T1>
        where T1 : notnull
        where T2 : notnull
    {
        protected AvroUnion() { }
        public AvroUnion(T1 value) : base(value) { }
        public AvroUnion(T2 value) => SetT2(value);
        public T2 GetT2() => Get<T2>(1);
        public void SetT2(T2 value) => Set(1, value);
        public static implicit operator T1(AvroUnion<T1, T2> union) => union.GetT1();
        public static implicit operator T2(AvroUnion<T1, T2> union) => union.GetT2();
        public static implicit operator AvroUnion<T1, T2>(T1 value) => new AvroUnion<T1, T2>(value);
        public static implicit operator AvroUnion<T1, T2>(T2 value) => new AvroUnion<T1, T2>(value);
    }

    public class AvroUnion<T1, T2, T3> :
        AvroUnion<T1, T2>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
    {
        protected AvroUnion() { }
        public AvroUnion(T1 value) : base(value) { }
        public AvroUnion(T2 value) : base(value) { }
        public AvroUnion(T3 value) => SetT3(value);
        public T3 GetT3() => Get<T3>(2);
        public void SetT3(T3 value) => Set(2, value);
        public static implicit operator T1(AvroUnion<T1, T2, T3> union) => union.GetT1();
        public static implicit operator T2(AvroUnion<T1, T2, T3> union) => union.GetT2();
        public static implicit operator T3(AvroUnion<T1, T2, T3> union) => union.GetT3();
        public static implicit operator AvroUnion<T1, T2, T3>(T1 value) => new AvroUnion<T1, T2, T3>(value);
        public static implicit operator AvroUnion<T1, T2, T3>(T2 value) => new AvroUnion<T1, T2, T3>(value);
        public static implicit operator AvroUnion<T1, T2, T3>(T3 value) => new AvroUnion<T1, T2, T3>(value);
    }

    public class AvroUnion<T1, T2, T3, T4> :
        AvroUnion<T1, T2, T3>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
        where T4 : notnull
    {
        protected AvroUnion() { }
        public AvroUnion(T1 value) : base(value) { }
        public AvroUnion(T2 value) : base(value) { }
        public AvroUnion(T3 value) : base(value) { }
        public AvroUnion(T4 value) => SetT4(value);
        public T4 GetT4() => Get<T4>(3);
        public void SetT4(T4 value) => Set(3, value);
        public static implicit operator T1(AvroUnion<T1, T2, T3, T4> union) => union.GetT1();
        public static implicit operator T2(AvroUnion<T1, T2, T3, T4> union) => union.GetT2();
        public static implicit operator T3(AvroUnion<T1, T2, T3, T4> union) => union.GetT3();
        public static implicit operator T4(AvroUnion<T1, T2, T3, T4> union) => union.GetT4();
        public static implicit operator AvroUnion<T1, T2, T3, T4>(T1 value) => new AvroUnion<T1, T2, T3, T4>(value);
        public static implicit operator AvroUnion<T1, T2, T3, T4>(T2 value) => new AvroUnion<T1, T2, T3, T4>(value);
        public static implicit operator AvroUnion<T1, T2, T3, T4>(T3 value) => new AvroUnion<T1, T2, T3, T4>(value);
        public static implicit operator AvroUnion<T1, T2, T3, T4>(T4 value) => new AvroUnion<T1, T2, T3, T4>(value);
    }

    public class AvroUnion<T1, T2, T3, T4, T5> :
        AvroUnion<T1, T2, T3, T4>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
        where T4 : notnull
        where T5 : notnull
    {
        protected AvroUnion() { }
        public AvroUnion(T1 value) : base(value) { }
        public AvroUnion(T2 value) : base(value) { }
        public AvroUnion(T3 value) : base(value) { }
        public AvroUnion(T4 value) : base(value) { }
        public AvroUnion(T5 value) => SetT5(value);
        public T5 GetT5() => Get<T5>(4);
        public void SetT5(T5 value) => Set(4, value);
        public static implicit operator T1(AvroUnion<T1, T2, T3, T4, T5> union) => union.GetT1();
        public static implicit operator T2(AvroUnion<T1, T2, T3, T4, T5> union) => union.GetT2();
        public static implicit operator T3(AvroUnion<T1, T2, T3, T4, T5> union) => union.GetT3();
        public static implicit operator T4(AvroUnion<T1, T2, T3, T4, T5> union) => union.GetT4();
        public static implicit operator T5(AvroUnion<T1, T2, T3, T4, T5> union) => union.GetT5();

        public static implicit operator AvroUnion<T1, T2, T3, T4, T5>(T1 value) => new AvroUnion<T1, T2, T3, T4, T5>(value);
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5>(T2 value) => new AvroUnion<T1, T2, T3, T4, T5>(value);
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5>(T3 value) => new AvroUnion<T1, T2, T3, T4, T5>(value);
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5>(T4 value) => new AvroUnion<T1, T2, T3, T4, T5>(value);
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5>(T5 value) => new AvroUnion<T1, T2, T3, T4, T5>(value);
    }
}
