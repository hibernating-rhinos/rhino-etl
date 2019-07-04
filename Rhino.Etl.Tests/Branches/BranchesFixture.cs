namespace Rhino.Etl.Tests.Branches
{
    using System;
    using System.Threading;
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.Infrastructure;
    using Xunit;
    using Xunit.Abstractions;

    public abstract class BranchesFixture : BaseFibonacciTest
    {
        protected BranchesFixture(TestDatabaseFixture testDatabase)
            : base(testDatabase)
        { }

       [Fact]
        public void CanBranchThePipeline()
        {
            using (var process = CreateBranchingProcess(30, 2))
            {
                process.Execute();
                Assert.Empty(process.GetAllErrors());
            }

            AssertFibonacci(30, 2);
        }

        [Fact] 
        public void CanBranchThePipelineEfficiently()
        {
            const int iterations = 1000;
            const int childOperations = 5;

            var initialMemory = GC.GetTotalMemory(true);

            using (var process = CreateBranchingProcess(iterations, childOperations))
            {
                process.Execute();
                Assert.Empty(process.GetAllErrors());
            }

            var finalMemory = GC.GetTotalMemory(true);
            var consumedMemory = finalMemory - initialMemory;
            var tooMuchMemory = Math.Pow(2, 20);
            
            Assert.True(consumedMemory < tooMuchMemory, "Consuming too much memory - (" + consumedMemory.ToString() + " >= " + tooMuchMemory + ")");

            // Wait to ensure that outcomes of multi-threaded database transactions
            // are visible to other transactions
            Thread.Sleep(200);

            AssertFibonacci(iterations, childOperations);
        }

        protected abstract EtlProcess CreateBranchingProcess(int iterations, int childOperations);

        protected void AssertFibonacci(int iterations, int repetitionsPerIteration)
        {
            AssertTotalItems(iterations * repetitionsPerIteration);
            AssertRepetitions(repetitionsPerIteration);
        }

        private void AssertRepetitions(int repetitionsPerIteration)
        {
            int wrongRepetitions = Use.Transaction(TestDatabase.ConnectionString, cmd =>
            {
                cmd.CommandText = $@"
                    SELECT count(*) 
                    FROM (
                        SELECT id, count(*) as count
                        FROM Fibonacci
                        GROUP BY id
                        HAVING count(*) <> {repetitionsPerIteration}
                    ) as ignored";
                return (int)cmd.ExecuteScalar();
            });

            Assert.Equal(1 /* 1 is repetated twice the others */, wrongRepetitions);
        }

        private void AssertTotalItems(int expectedCount)
        {
            int totalCount = Use.Transaction(TestDatabase.ConnectionString, cmd =>
                 {
                     cmd.CommandText = "SELECT count(*) FROM Fibonacci";
                     return (int) cmd.ExecuteScalar();
                 });
            
            Assert.Equal(expectedCount, totalCount);
        }
    }
}