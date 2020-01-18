using System;
using System.Collections;
using System.Collections.Generic;

namespace Avro.Monad
{
    public abstract class Maybe<T> : IEquatable<Maybe<T>>, IStructuralEquatable
    {
        private static readonly int SOME_HASH = HashCode.Combine("Some ");
        private static readonly int NONE_HASH = HashCode.Combine("None ");

        public static implicit operator Maybe<T>(T value) => Some(value);

        public static Maybe<T> Some(T value) => new Choices.Some(value);
        public static Maybe<T> None { get; } = new Choices.None();

        public abstract R Match<R>(Func<T, R> someFunc, Func<R> noneFunc);
        public abstract void Iter(Action<T> someAction, Action noneAction);

        public Maybe<R> Map<R>(Func<T, R> map) => Match(v => Maybe<R>.Some(map(v)), () => Maybe<R>.None);

        public R Fold<R>(Func<R, T, R> foldFunc, R seed) => Match(t => foldFunc(seed, t), () => seed);
        public R GetOrElse<R>(Func<T, R> foldFunc, R seed) => Fold((_, t) => foldFunc(t), seed);

        public static Maybe<T> Return(T value) => Some(value);
        public Maybe<R> Bind<R>(Func<T, Maybe<R>> map) => Match(v => map(v).Match(r => Maybe<R>.Some(r), () => Maybe<R>.None), () => Maybe<R>.None);

        #region Value Semantics
        public static bool operator ==(Maybe<T> x, Maybe<T> y) => x.Equals(y);
        public static bool operator !=(Maybe<T> x, Maybe<T> y) => !(x == y);

        bool IEquatable<Maybe<T>>.Equals(Maybe<T> other) => Equals(other);
        public abstract bool Equals(object other, IEqualityComparer comparer);
        public abstract int GetHashCode(IEqualityComparer comparer);
        #endregion

        #region Overrides
        public abstract override bool Equals(object obj);
        public abstract override int GetHashCode();
        public abstract override string ToString();
        #endregion

        private Maybe() { }

        private static class Choices
        {
            public class Some : Maybe<T>
            {
                private T Value { get; }
                public Some(T value) => Value = value;

                public override R Match<R>(Func<T, R> someFunc, Func<R> noneFunc) => someFunc(Value);
                public override void Iter(Action<T> someAction, Action noneAction) => someAction(Value);
                public override string ToString() => $"Some ({Value})";

                #region Value Semantics
                public override bool Equals(object obj) =>
                    obj switch
                    {
                        Some s => EqualityComparer<T>.Default.Equals(Value, s.Value),
                        _ => false
                    };
                public override bool Equals(object other, IEqualityComparer comparer) =>
                    other switch
                    {
                        Some s => comparer.Equals(Value, s.Value),
                        _ => false
                    };
                public override int GetHashCode() => SOME_HASH ^ HashCode.Combine(Value);
                public override int GetHashCode(IEqualityComparer comparer) => SOME_HASH ^ comparer.GetHashCode(Value);
                #endregion
            }

            public class None : Maybe<T>
            {
                public override R Match<R>(Func<T, R> someFunc, Func<R> noneFunc) => noneFunc();
                public override void Iter(Action<T> someAction, Action noneAction) => noneAction();
                public override string ToString() => "None";

                #region Value Semantics
                public override bool Equals(object obj) => obj is None;
                public override int GetHashCode() => NONE_HASH;
                public override bool Equals(object other, IEqualityComparer comparer) => Equals(other);
                public override int GetHashCode(IEqualityComparer comparer) => GetHashCode();
                #endregion
            }
        }
    }
}
