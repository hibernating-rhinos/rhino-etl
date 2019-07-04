namespace Rhino.Etl.Tests.Branches
{
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.Operations;
    using Rhino.Etl.Core.Pipelines;

    public class SingleThreadedFibonacciBranchingProcess : AbstractFibonacciBranchingProcess
    {
        public SingleThreadedFibonacciBranchingProcess(string connectionStringName, int numberOfFibonacciIterations, int numberOfChildOperations)
            : base(connectionStringName, numberOfFibonacciIterations, numberOfChildOperations)
        { }

        protected override AbstractBranchingOperation CreateBranchingOperation()
        {
            return new BranchingOperationWithBug();
        }

        protected override IPipelineExecuter CreatePipelineExecuter()
        {
            return new SingleThreadedPipelineExecuter();
        }
    }
}