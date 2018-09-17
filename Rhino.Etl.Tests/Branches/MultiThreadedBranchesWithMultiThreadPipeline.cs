namespace Rhino.Etl.Tests.Branches
{
    using Rhino.Etl.Core;

    public class MultiThreadedBranchesWithMultiThreadPipeline : BranchesFixture
    {
        public MultiThreadedBranchesWithMultiThreadPipeline(TestDatabaseFixture testDatabase) 
            : base(testDatabase)
        { }

        protected override EtlProcess CreateBranchingProcess(int iterations, int childOperations)
        {
            return new MultiThreadedWithMultiThreadPipelineFibonacciBranchingProcess(TestDatabase.ConnectionStringName, iterations, childOperations);
        }
    }
}