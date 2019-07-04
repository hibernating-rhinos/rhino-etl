namespace Rhino.Etl.Tests.Branches
{
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.Operations;
    using Rhino.Etl.Core.Pipelines;

    public class MultiThreadedWithSingleThreadPipelineFibonacciBranchingProcess : AbstractFibonacciBranchingProcess
    {
        public MultiThreadedWithSingleThreadPipelineFibonacciBranchingProcess(string connectionStringName, int numberOfFibonacciIterations, int numberOfChildOperations)
            : base(connectionStringName, numberOfFibonacciIterations, numberOfChildOperations)
        { }

        protected override AbstractBranchingOperation CreateBranchingOperation()
        {
            return new MultiThreadedBranchingOperation();
        }

        protected override IPipelineExecuter CreatePipelineExecuter()
        {
            return new SingleThreadedNonCachedPipelineExecuter();
        }
    }
}