using System;

namespace Avro.Monad
{
    public static partial class LinqExtensions
    {
        public static Maybe<C> SelectMany<A, B, C>(this Maybe<A> ma, Func<A, Maybe<B>> f, Func<A, B, C> select) => ma.Bind(a => f(a).Map(b => select(a, b)));
    }
}
