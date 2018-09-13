#if FEATURE_SQLCOMMANDSET
namespace Rhino.Etl.Tests
{
    using System;
    using System.Collections.Generic;
    using Rhino.Etl.Tests.Fibonacci.Batch;
    using Rhino.Etl.Tests.Fibonacci.Output;
    using Xunit;

    public class SqlBatchOperationFixture : BaseFibonacciTest
    {
        [Fact]
        public void CanInsertToDatabaseFromInMemoryCollection()
        {
            BatchFibonacci fibonaci = new BatchFibonacci(25, Should.WorkFine);
            fibonaci.Execute();

            Assert25ThFibonacci();
        }

        [Fact]
        public void CanInsertToDatabaseFromInMemoryCollectionWithSlowOperation()
        {
            var fibonaci = new SlowBatchFibonacci(25, Should.WorkFine);
            fibonaci.Execute();

            Assert25ThFibonacci();
        }

        [Fact]
        public void CanInsertToDatabaseFromConnectionStringSettingsAndInMemoryCollection()
        {
            BatchFibonacciFromConnectionStringSettings fibonaci = new BatchFibonacciFromConnectionStringSettings(25, Should.WorkFine);
            fibonaci.Execute();

            Assert25ThFibonacci();
        }

        [Fact]
        public void WhenErrorIsThrownWillRollbackTransaction()
        {
            BatchFibonacci fibonaci = new BatchFibonacci(25, Should.Throw);
            fibonaci.Execute();
            Assert.Single(new List<Exception>(fibonaci.GetAllErrors()));
            AssertFibonacciTableEmpty();
        }
    }
}
#endif