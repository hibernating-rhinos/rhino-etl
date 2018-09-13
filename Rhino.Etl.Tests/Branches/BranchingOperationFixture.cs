namespace Rhino.Etl.Tests.Branches
{
    using System.Linq;
    using Rhino.Etl.Core.Operations;
    using Xunit;

    public class BranchingOperationFixture
    {
        [Fact]
        public void TheOldBranchingOperationDoesNotReportErrors()
        {
            using (var process = new BranchingOperationProcess<BranchingOperationWithBug>())
            {
                process.Execute();
                var errors = process.GetAllErrors().Count();
                Assert.Equal(0, errors);
            }
        }

        [Fact]
        public void TheNewBranchingOperationReportsErrors()
        {
            using (var process = new BranchingOperationProcess<BranchingOperation>())
            {
                process.Execute();
                var errors = process.GetAllErrors().Count();
                Assert.NotEqual(0, errors);
            }
        }
    }
}