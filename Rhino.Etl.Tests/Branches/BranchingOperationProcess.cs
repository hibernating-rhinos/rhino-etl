namespace Rhino.Etl.Tests.Branches
{
    using System;
    using System.Collections.Generic;
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.Operations;

    internal class BranchingOperationProcess<T> : EtlProcess where T : AbstractBranchingOperation, new()
    {
        protected override void Initialize()
        {
            Register(new GenerateTuples());
            Register(new T()
              .Add(Partial
                 .Register(new Add(false)))
              .Add(Partial
                 .Register(new Subtract(true))));
        }
    }

    internal class GenerateTuples : AbstractOperation
    {
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            for (int i = 0; i < 2; i++)
            {
                for (int j = 0; j < 2; j++)
                {
                    var row = new Row();
                    row["a"] = i;
                    row["b"] = j;
                    yield return row;
                }
            }
        }
    }

    internal class Subtract : AbstractOperationWithErrors
    {
        public Subtract(bool withErrors)
            : base(withErrors)
        { }

        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (var row in rows)
            {
                var a = row["a"] as int?;
                var b = row["b"] as int?;
                row["operation"] = "subtract";
                row["result"] = a - b;
                if (withErrors) { Error(new ApplicationException(), "Error in Subtract"); }
                yield return row;
            }
        }
    }

    internal abstract class AbstractOperationWithErrors : AbstractOperation
    {
        protected readonly bool withErrors;

        public AbstractOperationWithErrors(bool withErrors)
        {
            this.withErrors = withErrors;
        }
    }

    internal class Add : AbstractOperationWithErrors
    {
        public Add(bool withErrors)
            : base(withErrors)
        { }

        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (var row in rows)
            {
                var a = row["a"] as int?;
                var b = row["b"] as int?;
                row["operation"] = "add";
                row["result"] = a + b;

                if (withErrors) { Error(new ApplicationException(), "Error in Add"); }

                yield return row;
            }
        }
    }
}