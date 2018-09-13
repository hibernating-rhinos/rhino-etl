namespace Rhino.Etl.Tests
{
    using System;
    using System.Collections.Generic;
    using Rhino.Etl.Tests.Fibonacci.Bulk;
    using Rhino.Etl.Tests.Fibonacci.Output;
    using Xunit;

    public class SqlBulkInsertOperationFixture : BaseFibonacciTest
    {
        [Fact]
        public void CanInsertToDatabaseFromInMemoryCollection()
        {
            BulkInsertFibonacciToDatabase fibonacci = new BulkInsertFibonacciToDatabase(25, Should.WorkFine);
            fibonacci.Execute();

            Assert25ThFibonacci();
        }

        [Fact]
        public void CanInsertToDatabaseFromConnectionStringSettingsAndInMemoryCollection()
        {
            BulkInsertFibonacciToDatabaseFromConnectionStringSettings fibonacci = new BulkInsertFibonacciToDatabaseFromConnectionStringSettings(25, Should.WorkFine);
            fibonacci.Execute();

            Assert25ThFibonacci();
        }

        [Fact]
        public void WhenErrorIsThrownWillRollbackTransaction()
        {
            BulkInsertFibonacciToDatabase fibonaci = new BulkInsertFibonacciToDatabase(25, Should.Throw);
            fibonaci.Execute();
            Assert.Single(new List<Exception>(fibonaci.GetAllErrors()));
            AssertFibonacciTableEmpty();
        }
    }

    public class BulkInsertNotificationTests : BaseFibonacciTest
    {
        [Fact]
        public void CheckNotifyBatchSizeTakenFromBatchSize()
        {
            FibonacciBulkInsert fibonacci = new FibonacciBulkInsert();
            fibonacci.BatchSize = 50;

            Assert.Equal(fibonacci.BatchSize, fibonacci.NotifyBatchSize);
        }

        [Fact]
        public void CheckNotifyBatchSizeNotTakenFromBatchSize()
        {
            FibonacciBulkInsert fibonacci = new FibonacciBulkInsert();
            fibonacci.BatchSize = 50;
            fibonacci.NotifyBatchSize = 25;

            Assert.Equal(25, fibonacci.NotifyBatchSize);
        }
    }
}
