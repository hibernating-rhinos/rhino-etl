namespace Rhino.Etl.Tests.Branches
{
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.Operations;
    using Rhino.Etl.Tests.Fibonacci;
    using Rhino.Etl.Tests.Fibonacci.Bulk;
    using Xunit;

    [Collection(FibonacciTestCollection.Name)]
    public abstract class AbstractFibonacciBranchingProcess : EtlProcess
    {
        private readonly int _numberOfFibonacciIterations;
        private readonly int _numberOfChildOperations;
        private readonly string _connectionStringName;

        protected AbstractFibonacciBranchingProcess(string connectionStringName, int numberOfFibonacciIterations, int numberOfChildOperations)
        {
            _connectionStringName = connectionStringName;
            _numberOfFibonacciIterations = numberOfFibonacciIterations;
            _numberOfChildOperations = numberOfChildOperations;
        }

        protected override void Initialize()
        {
            PipelineExecuter = CreatePipelineExecuter();

            Register(new FibonacciOperation(_numberOfFibonacciIterations));

            var split = CreateBranchingOperation();
            for (int i = 0; i < _numberOfChildOperations; i++)
            {
                split.Add(new FibonacciBulkInsert(_connectionStringName));
            }
            Register(split);
        }

        protected abstract AbstractBranchingOperation CreateBranchingOperation();

        protected abstract IPipelineExecuter CreatePipelineExecuter();
    }
}