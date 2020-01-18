using Avro.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Text;

namespace Avro.Monad
{
    public abstract class ReadExpr
    {
        public int Foo()
        {
            var x = Maybe<int>.Some(1);

            return x.GetOrElse(x => x, 0);
        }
    }
}
