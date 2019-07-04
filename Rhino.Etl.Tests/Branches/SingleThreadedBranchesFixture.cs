namespace Rhino.Etl.Tests.Branches
{
    using Rhino.Etl.Core;

    public class SingleThreadedBranchesFixture : BranchesFixture
    {
        public SingleThreadedBranchesFixture(TestDatabaseFixture testDatabase) 
            : base(testDatabase)
        { }

        protected override EtlProcess CreateBranchingProcess(int iterations, int childOperations)
        {
            return new SingleThreadedFibonacciBranchingProcess(TestDatabase.ConnectionStringName, iterations, childOperations);
        }
    }
}