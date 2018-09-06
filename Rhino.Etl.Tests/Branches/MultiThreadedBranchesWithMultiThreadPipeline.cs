namespace Rhino.Etl.Tests.Branches
{
    using Rhino.Etl.Core;

    public class MultiThreadedBranchesWithMultiThreadPipeline : BranchesFixture
    {
        protected override EtlProcess CreateBranchingProcess(int iterations, int childOperations)
        {
            return new MultiThreadedWithMultiThreadPipelineFibonacciBranchingProcess(iterations, childOperations);
        }
    }
}