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
        public SqlBatchOperationFixture(TestDatabaseFixture testDatabase) 
            : base(testDatabase)
        { }

        [Fact]
        public void CanInsertToDatabaseFromInMemoryCollection()
        {
            using (var fibonacci = new BatchFibonacci(TestDatabase.ConnectionStringName, 25, Should.WorkFine))
            {
                fibonacci.Execute();
            }

            Assert25ThFibonacci();
        }

        [Fact]
        public void CanInsertToDatabaseFromInMemoryCollectionWithSlowOperation()
        {
            using (var fibonacci = new SlowBatchFibonacci(TestDatabase.ConnectionStringName, 25, Should.WorkFine))
            {
                fibonacci.Execute();
            }

            Assert25ThFibonacci();
        }

        [Fact]
        public void CanInsertToDatabaseFromConnectionStringSettingsAndInMemoryCollection()
        {
            using (var fibonacci = new BatchFibonacci(TestDatabase.ConnectionString, 25, Should.WorkFine))
            {
                fibonacci.Execute();
            }

            Assert25ThFibonacci();
        }

        [Fact]
        public void WhenErrorIsThrownWillRollbackTransaction()
        {
            using (var fibonacci = new BatchFibonacci(TestDatabase.ConnectionStringName,  25, Should.Throw))
            {
                fibonacci.Execute();
                Assert.Single(new List<Exception>(fibonacci.GetAllErrors()));
            }

            AssertFibonacciTableEmpty();
        }
    }
}
#endif