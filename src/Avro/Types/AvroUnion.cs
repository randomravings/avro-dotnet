using System;

namespace Avro.Types
{
    public class AvroUnion<T1>
    {
        protected Type _type;
        protected object _value;
        protected byte _index;

        public AvroUnion()
            : base()
        {
            _type = typeof(T1);
            _value = default(T1);
            _index = 0;
        }
        public AvroUnion(T1 value)
        {
            SetT1(value);
        }
        public Type Type => _type;
        public long Index => _index;
        public T1 GetT1()
        {
            if (_index != 0)
                throw new InvalidCastException($"Union is instance of '{_type.FullName}'");
            return (T1)_value;
        }
        public void SetT1(T1 value)
        {
            _type = typeof(T1);
            _index = 0;
            _value = value;
        }

        public static implicit operator T1(AvroUnion<T1> union) => union.GetT1();
        public static implicit operator AvroUnion<T1>(T1 value) => new AvroUnion<T1>(value);
    }

    public class AvroUnion<T1, T2> : AvroUnion<T1>
    {
        public AvroUnion()
            : base() { }
        public AvroUnion(T1 value)
            : base(value) { }
        public AvroUnion(T2 value)
        {
            SetT2(value);
        }
        public T2 GetT2()
        {
            if (_index != 1)
                throw new InvalidCastException($"Union is instance of '{_type.FullName}'");
            return (T2)_value;
        }
        public void SetT2(T2 value)
        {
            _type = typeof(T2);
            _index = 1;
            _value = value;
        }
        public static implicit operator T1(AvroUnion<T1, T2> union) => union.GetT1();
        public static implicit operator AvroUnion<T1, T2>(T1 value) => new AvroUnion<T1, T2>(value);
        public static implicit operator T2(AvroUnion<T1, T2> union) => union.GetT2();
        public static implicit operator AvroUnion<T1, T2>(T2 value) => new AvroUnion<T1, T2>(value);
    }

    public class AvroUnion<T1, T2, T3> : AvroUnion<T1, T2>
    {
        public AvroUnion()
            : base() { }
        public AvroUnion(T1 value)
            : base(value) { }
        public AvroUnion(T2 value)
            : base(value) { }
        public AvroUnion(T3 value)
        {
            SetT3(value);
        }
        public T3 GetT3()
        {
            if (_index != 2)
                throw new InvalidCastException($"Union is instance of '{_type.FullName}'");
            return (T3)_value;
        }
        public void SetT3(T3 value)
        {
            _type = typeof(T2);
            _index = 2;
            _value = value;
        }
        public static implicit operator T1(AvroUnion<T1, T2, T3> union) => union.GetT1();
        public static implicit operator AvroUnion<T1, T2, T3>(T1 value) => new AvroUnion<T1, T2, T3>(value);
        public static implicit operator T2(AvroUnion<T1, T2, T3> union) => union.GetT2();
        public static implicit operator AvroUnion<T1, T2, T3>(T2 value) => new AvroUnion<T1, T2, T3>(value);
        public static implicit operator T3(AvroUnion<T1, T2, T3> union) => union.GetT3();
        public static implicit operator AvroUnion<T1, T2, T3>(T3 value) => new AvroUnion<T1, T2, T3>(value);
    }

    public class AvroUnion<T1, T2, T3, T4> : AvroUnion<T1, T2, T3>
    {
        public AvroUnion()
            : base() { }
        public AvroUnion(T1 value)
            : base(value) { }
        public AvroUnion(T2 value)
            : base(value) { }
        public AvroUnion(T3 value)
            : base(value) { }
        public AvroUnion(T4 value)
        {
            SetT4(value);
        }
        public T4 GetT4()
        {
            if (_index != 3)
                throw new InvalidCastException($"Union is instance of '{_type.FullName}'");
            return (T4)_value;
        }
        public void SetT4(T4 value)
        {
            _type = typeof(T4);
            _index = 3;
            _value = value;
        }
        public static implicit operator T1(AvroUnion<T1, T2, T3, T4> union) => union.GetT1();
        public static implicit operator AvroUnion<T1, T2, T3, T4>(T1 value) => new AvroUnion<T1, T2, T3, T4>(value);
        public static implicit operator T2(AvroUnion<T1, T2, T3, T4> union) => union.GetT2();
        public static implicit operator AvroUnion<T1, T2, T3, T4>(T2 value) => new AvroUnion<T1, T2, T3, T4>(value);
        public static implicit operator T3(AvroUnion<T1, T2, T3, T4> union) => union.GetT3();
        public static implicit operator AvroUnion<T1, T2, T3, T4>(T3 value) => new AvroUnion<T1, T2, T3, T4>(value);
        public static implicit operator T4(AvroUnion<T1, T2, T3, T4> union) => union.GetT4();
        public static implicit operator AvroUnion<T1, T2, T3, T4>(T4 value) => new AvroUnion<T1, T2, T3, T4>(value);
    }

    public class AvroUnion<T1, T2, T3, T4, T5> : AvroUnion<T1, T2, T3, T4>
    {
        public AvroUnion()
            : base() { }
        public AvroUnion(T1 value)
            : base(value) { }
        public AvroUnion(T2 value)
            : base(value) { }
        public AvroUnion(T3 value)
            : base(value) { }
        public AvroUnion(T4 value)
            : base(value) { }
        public AvroUnion(T5 value)
        {
            _type = typeof(T5);
            _value = value;
        }
        public T5 GetT5()
        {
            if (_index != 4)
                throw new InvalidCastException($"Union is instance of '{_type.FullName}'");
            return (T5)_value;
        }
        public void SetT5(T5 value)
        {
            _type = typeof(T5);
            _index = 4;
            _value = value;
        }
        public static implicit operator T1(AvroUnion<T1, T2, T3, T4, T5> union) => union.GetT1();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5>(T1 value) => new AvroUnion<T1, T2, T3, T4, T5>(value);
        public static implicit operator T2(AvroUnion<T1, T2, T3, T4, T5> union) => union.GetT2();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5>(T2 value) => new AvroUnion<T1, T2, T3, T4, T5>(value);
        public static implicit operator T3(AvroUnion<T1, T2, T3, T4, T5> union) => union.GetT3();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5>(T3 value) => new AvroUnion<T1, T2, T3, T4, T5>(value);
        public static implicit operator T4(AvroUnion<T1, T2, T3, T4, T5> union) => union.GetT4();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5>(T4 value) => new AvroUnion<T1, T2, T3, T4, T5>(value);
        public static implicit operator T5(AvroUnion<T1, T2, T3, T4, T5> union) => union.GetT5();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5>(T5 value) => new AvroUnion<T1, T2, T3, T4, T5>(value);
    }

    public class AvroUnion<T1, T2, T3, T4, T5, T6> : AvroUnion<T1, T2, T3, T4, T5>
    {
        public AvroUnion()
            : base() { }
        public AvroUnion(T1 value)
            : base(value) { }
        public AvroUnion(T2 value)
            : base(value) { }
        public AvroUnion(T3 value)
            : base(value) { }
        public AvroUnion(T4 value)
            : base(value) { }
        public AvroUnion(T5 value)
            : base(value) { }
        public AvroUnion(T6 value)
        {
            SetT6(value);
        }
        public T6 GetT6()
        {
            if (_index != 5)
                throw new InvalidCastException($"Union is instance of '{_type.FullName}'");
            return (T6)_value;
        }
        public void SetT6(T6 value)
        {
            _type = typeof(T6);
            _index = 5;
            _value = value;
        }
        public static implicit operator T1(AvroUnion<T1, T2, T3, T4, T5, T6> union) => union.GetT1();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6>(T1 value) => new AvroUnion<T1, T2, T3, T4, T5, T6>(value);
        public static implicit operator T2(AvroUnion<T1, T2, T3, T4, T5, T6> union) => union.GetT2();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6>(T2 value) => new AvroUnion<T1, T2, T3, T4, T5, T6>(value);
        public static implicit operator T3(AvroUnion<T1, T2, T3, T4, T5, T6> union) => union.GetT3();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6>(T3 value) => new AvroUnion<T1, T2, T3, T4, T5, T6>(value);
        public static implicit operator T4(AvroUnion<T1, T2, T3, T4, T5, T6> union) => union.GetT4();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6>(T4 value) => new AvroUnion<T1, T2, T3, T4, T5, T6>(value);
        public static implicit operator T5(AvroUnion<T1, T2, T3, T4, T5, T6> union) => union.GetT5();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6>(T5 value) => new AvroUnion<T1, T2, T3, T4, T5, T6>(value);
        public static implicit operator T6(AvroUnion<T1, T2, T3, T4, T5, T6> union) => union.GetT6();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6>(T6 value) => new AvroUnion<T1, T2, T3, T4, T5, T6>(value);
    }

    public class AvroUnion<T1, T2, T3, T4, T5, T6, T7> : AvroUnion<T1, T2, T3, T4, T5, T6>
    {
        public AvroUnion()
            : base() { }
        public AvroUnion(T1 value)
            : base(value) { }
        public AvroUnion(T2 value)
            : base(value) { }
        public AvroUnion(T3 value)
            : base(value) { }
        public AvroUnion(T4 value)
            : base(value) { }
        public AvroUnion(T5 value)
            : base(value) { }
        public AvroUnion(T6 value)
            : base(value) { }
        public AvroUnion(T7 value)
        {
            SetT7(value);
        }
        public T7 GetT7()
        {
            if (_index != 6)
                throw new InvalidCastException($"Union is instance of '{_type.FullName}'");
            return (T7)_value;
        }
        public void SetT7(T7 value)
        {
            _type = typeof(T7);
            _index = 6;
            _value = value;
        }
        public static implicit operator T1(AvroUnion<T1, T2, T3, T4, T5, T6, T7> union) => union.GetT1();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7>(T1 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(value);
        public static implicit operator T2(AvroUnion<T1, T2, T3, T4, T5, T6, T7> union) => union.GetT2();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7>(T2 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(value);
        public static implicit operator T3(AvroUnion<T1, T2, T3, T4, T5, T6, T7> union) => union.GetT3();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7>(T3 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(value);
        public static implicit operator T4(AvroUnion<T1, T2, T3, T4, T5, T6, T7> union) => union.GetT4();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7>(T4 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(value);
        public static implicit operator T5(AvroUnion<T1, T2, T3, T4, T5, T6, T7> union) => union.GetT5();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7>(T5 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(value);
        public static implicit operator T6(AvroUnion<T1, T2, T3, T4, T5, T6, T7> union) => union.GetT6();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7>(T6 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(value);
        public static implicit operator T7(AvroUnion<T1, T2, T3, T4, T5, T6, T7> union) => union.GetT7();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7>(T7 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7>(value);
    }

    public class AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> : AvroUnion<T1, T2, T3, T4, T5, T6, T7>
    {
        public AvroUnion()
            : base() { }
        public AvroUnion(T1 value)
            : base(value) { }
        public AvroUnion(T2 value)
            : base(value) { }
        public AvroUnion(T3 value)
            : base(value) { }
        public AvroUnion(T4 value)
            : base(value) { }
        public AvroUnion(T5 value)
            : base(value) { }
        public AvroUnion(T6 value)
            : base(value) { }
        public AvroUnion(T7 value)
            : base(value) { }
        public AvroUnion(T8 value)
        {
            _type = typeof(T8);
            _value = value;
        }
        public T8 GetT8()
        {
            if (_index != 7)
                throw new InvalidCastException($"Union is instance of '{_type.FullName}'");
            return (T8)_value;
        }
        public void SetT8(T8 value)
        {
            _type = typeof(T8);
            _index = 7;
            _value = value;
        }
        public static implicit operator T1(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> union) => union.GetT1();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(T1 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(value);
        public static implicit operator T2(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> union) => union.GetT2();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(T2 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(value);
        public static implicit operator T3(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> union) => union.GetT3();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(T3 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(value);
        public static implicit operator T4(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> union) => union.GetT4();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(T4 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(value);
        public static implicit operator T5(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> union) => union.GetT5();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(T5 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(value);
        public static implicit operator T6(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> union) => union.GetT6();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(T6 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(value);
        public static implicit operator T7(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> union) => union.GetT7();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(T7 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(value);
        public static implicit operator T8(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8> union) => union.GetT8();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(T8 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>(value);
    }

    public class AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> : AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8>
    {
        public AvroUnion()
            : base() { }
        public AvroUnion(T1 value)
            : base(value) { }
        public AvroUnion(T2 value)
            : base(value) { }
        public AvroUnion(T3 value)
            : base(value) { }
        public AvroUnion(T4 value)
            : base(value) { }
        public AvroUnion(T5 value)
            : base(value) { }
        public AvroUnion(T6 value)
            : base(value) { }
        public AvroUnion(T7 value)
            : base(value) { }
        public AvroUnion(T8 value)
            : base(value) { }
        public AvroUnion(T9 value)
        {
            SetT9(value);
        }
        public T9 GetT9()
        {
            if (_index != 8)
                throw new InvalidCastException($"Union is instance of '{_type.FullName}'");
            return (T9)_value;
        }
        public void SetT9(T9 value)
        {
            _type = typeof(T9);
            _index = 7;
            _value = value;
        }
        public static implicit operator T1(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> union) => union.GetT1();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(T1 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(value);
        public static implicit operator T2(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> union) => union.GetT2();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(T2 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(value);
        public static implicit operator T3(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> union) => union.GetT3();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(T3 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(value);
        public static implicit operator T4(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> union) => union.GetT4();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(T4 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(value);
        public static implicit operator T5(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> union) => union.GetT5();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(T5 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(value);
        public static implicit operator T6(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> union) => union.GetT6();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(T6 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(value);
        public static implicit operator T7(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> union) => union.GetT7();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(T7 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(value);
        public static implicit operator T8(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> union) => union.GetT8();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(T8 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(value);
        public static implicit operator T9(AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9> union) => union.GetT9();
        public static implicit operator AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(T9 value) => new AvroUnion<T1, T2, T3, T4, T5, T6, T7, T8, T9>(value);
    }
}
