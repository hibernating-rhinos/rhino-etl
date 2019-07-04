namespace Rhino.Etl.Tests
{
    using System;
    using System.Collections.Generic;
    using Rhino.Etl.Tests.Fibonacci.Bulk;
    using Rhino.Etl.Tests.Fibonacci.Output;
    using Xunit;

    public class SqlBulkInsertOperationFixture : BaseFibonacciTest
    {
        public SqlBulkInsertOperationFixture(TestDatabaseFixture testDatabase) 
            : base(testDatabase)
        { }

        [Fact]
        public void CanInsertToDatabaseFromInMemoryCollection()
        {
            using (var fibonacci = new BulkInsertFibonacciToDatabase(TestDatabase.ConnectionStringName, 25, Should.WorkFine))
            {
                fibonacci.Execute();
            }

            Assert25ThFibonacci();
        }

        [Fact]
        public void CanInsertToDatabaseFromConnectionStringSettingsAndInMemoryCollection()
        {
            using (var fibonacci = new BulkInsertFibonacciToDatabase(TestDatabase.ConnectionString, 25, Should.WorkFine))
            {
                fibonacci.Execute();
            }

            Assert25ThFibonacci();
        }

        [Fact]
        public void WhenErrorIsThrownWillRollbackTransaction()
        {
            using (var fibonacci = new BulkInsertFibonacciToDatabase(TestDatabase.ConnectionString, 25, Should.Throw))
            {
                fibonacci.Execute();
                Assert.Single(new List<Exception>(fibonacci.GetAllErrors()));
            }

            AssertFibonacciTableEmpty();
        }
    }

    public class BulkInsertNotificationTests : BaseFibonacciTest
    {
        public BulkInsertNotificationTests(TestDatabaseFixture testDatabase) 
            : base(testDatabase)
        { }

        [Fact]
        public void CheckNotifyBatchSizeTakenFromBatchSize()
        {
            using (var fibonacci = new FibonacciBulkInsert(TestDatabase.ConnectionStringName))
            {
                fibonacci.BatchSize = 50;
                Assert.Equal(fibonacci.BatchSize, fibonacci.NotifyBatchSize);
            }
        }

        [Fact]
        public void CheckNotifyBatchSizeNotTakenFromBatchSize()
        {
            using (var fibonacci = new FibonacciBulkInsert(TestDatabase.ConnectionStringName))
            {
                fibonacci.BatchSize = 50;
                fibonacci.NotifyBatchSize = 25;
                Assert.Equal(25, fibonacci.NotifyBatchSize);
            }
        }
    }
}
