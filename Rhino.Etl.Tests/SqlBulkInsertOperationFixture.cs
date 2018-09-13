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
            using (var fibonacci = new BulkInsertFibonacciToDatabase(25, Should.WorkFine))
            {
                fibonacci.Execute();
                Assert25ThFibonacci();
            }
        }

        [Fact]
        public void CanInsertToDatabaseFromConnectionStringSettingsAndInMemoryCollection()
        {
            using (var fibonacci = new BulkInsertFibonacciToDatabaseFromConnectionStringSettings(25, Should.WorkFine))
            {
                fibonacci.Execute();
                Assert25ThFibonacci();
            }
        }

        [Fact]
        public void WhenErrorIsThrownWillRollbackTransaction()
        {
            using (var fibonacci = new BulkInsertFibonacciToDatabase(25, Should.Throw))
            {
                fibonacci.Execute();
                Assert.Single(new List<Exception>(fibonacci.GetAllErrors()));
                AssertFibonacciTableEmpty();
            }
        }
    }

    public class BulkInsertNotificationTests : BaseFibonacciTest
    {
        [Fact]
        public void CheckNotifyBatchSizeTakenFromBatchSize()
        {
            using (var fibonacci = new FibonacciBulkInsert())
            {
                fibonacci.BatchSize = 50;
                Assert.Equal(fibonacci.BatchSize, fibonacci.NotifyBatchSize);
            }
        }

        [Fact]
        public void CheckNotifyBatchSizeNotTakenFromBatchSize()
        {
            using (var fibonacci = new FibonacciBulkInsert())
            {
                fibonacci.BatchSize = 50;
                fibonacci.NotifyBatchSize = 25;
                Assert.Equal(25, fibonacci.NotifyBatchSize);
            }
        }
    }
}
