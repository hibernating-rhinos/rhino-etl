namespace Rhino.Etl.Tests.Branches
{
    using Rhino.Etl.Core;

    public class MultiThreadedBranchesWithSingleThreadPipeline : BranchesFixture
    {
        protected override EtlProcess CreateBranchingProcess(int iterations, int childOperations)
        {
            return new MultiThreadedWithSingleThreadPipelineFibonacciBranchingProcess(iterations, childOperations);
        }
    }
}