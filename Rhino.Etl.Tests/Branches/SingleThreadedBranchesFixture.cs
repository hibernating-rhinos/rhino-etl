namespace Rhino.Etl.Tests.Branches
{
    using Rhino.Etl.Core;

    public class SingleThreadedBranchesFixture : BranchesFixture
    {
        protected override EtlProcess CreateBranchingProcess(int iterations, int childOperations)
        {
            return new SingleThreadedFibonacciBranchingProcess(iterations, childOperations);
        }
    }
}