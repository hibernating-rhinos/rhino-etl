namespace Rhino.Etl.Tests.Branches
{
    using System;
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.Infrastructure;
    using Xunit;

    public abstract class BranchesFixture : BaseFibonacciTest
   {
        [Fact]
        public void CanBranchThePipeline()
        {
            using (var process = CreateBranchingProcess(30, 2))
                process.Execute();

            AssertFibonacci(30, 2);
        }

        [Fact(Skip = "Takes too long for multi-threaded scenarios")] 
        public void CanBranchThePipelineEfficiently()
        {
            const int iterations = 30000;
            const int childOperations = 10;

            var initialMemory = GC.GetTotalMemory(true);

            using (var process = CreateBranchingProcess(iterations, childOperations))
                process.Execute();

            var finalMemory = GC.GetTotalMemory(true);
            var consumedMemory = finalMemory - initialMemory;
            var tooMuchMemory = Math.Pow(2, 20);
            
            Assert.True(consumedMemory < tooMuchMemory, "Consuming too much memory - (" + consumedMemory.ToString() + " >= " + tooMuchMemory + ")");
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
            int wrongRepetitions = Use.Transaction("test", cmd =>
            {
                cmd.CommandText =
string.Format(@"    SELECT count(*) 
    FROM (
        SELECT id, count(*) as count
        FROM Fibonacci
        GROUP BY id
        HAVING count(*) <> {0}
    ) as ignored", repetitionsPerIteration);
                return (int)cmd.ExecuteScalar();
            });

            Assert.Equal(1 /* 1 is repetated twice the others */, wrongRepetitions);
        }

        private void AssertTotalItems(int expectedCount)
        {
            int totalCount = Use.Transaction("test", cmd =>
                 {
                     cmd.CommandText = "SELECT count(*) FROM Fibonacci";
                     return (int) cmd.ExecuteScalar();
                 });
            
            Assert.Equal(expectedCount, totalCount);
        }
    }
}