namespace Rhino.Etl.Tests
{
    using System;
    using System.Collections.Generic;
    using Rhino.Etl.Tests.Fibonacci.Output;
    using Xunit;
    
    public class OutputCommandFixture : BaseFibonacciTest
    {
        /// <inheritdoc />
        public OutputCommandFixture(TestDatabaseFixture testDatabase) 
            : base(testDatabase)
        { }

        [Fact]
        public void CanInsertToDatabaseFromInMemoryCollection()
        {
            using (var fibonacci = new OutputFibonacciToDatabase(TestDatabase.ConnectionStringName, 25, Should.WorkFine))
            {
                fibonacci.Execute();
            }

            Assert25ThFibonacci();
        }

        [Fact]
        public void CanInsertToDatabaseFromConnectionStringSettingsAndInMemoryCollection()
        {
            using (var fibonacci = new OutputFibonacciToDatabase(TestDatabase.ConnectionString, 25, Should.WorkFine))
            {
                fibonacci.Execute();
            }

            Assert25ThFibonacci();
        }

        [Fact]
        public void WillRaiseRowProcessedEvent()
        {
            int rowsProcessed = 0;

            using (OutputFibonacciToDatabase fibonacci = new OutputFibonacciToDatabase(TestDatabase.ConnectionStringName, 1, Should.WorkFine))
            {
                fibonacci.OutputOperation.OnRowProcessed += delegate { rowsProcessed++; };
                fibonacci.Execute();
            }

            Assert.Equal(1, rowsProcessed);
        }

        [Fact]
        public void WillRaiseRowProcessedEventUntilItThrows()
        {
            int rowsProcessed = 0;

            using (OutputFibonacciToDatabase fibonacci = new OutputFibonacciToDatabase(TestDatabase.ConnectionStringName, 25, Should.Throw))
            {
                fibonacci.OutputOperation.OnRowProcessed += delegate { rowsProcessed++; };
                fibonacci.Execute();

                Assert.Equal(fibonacci.ThrowingOperation.RowsAfterWhichToThrow, rowsProcessed);
            }
        }

        [Fact]
        public void WillRaiseFinishedProcessingEventOnce()
        {
            int finished = 0;

            using (OutputFibonacciToDatabase fibonacci = new OutputFibonacciToDatabase(TestDatabase.ConnectionStringName, 1, Should.WorkFine))
            {
                fibonacci.OutputOperation.OnFinishedProcessing += delegate { finished++; };
                fibonacci.Execute();
            }

            Assert.Equal(1, finished);
        }

        [Fact]
        public void WhenErrorIsThrownWillRollbackTransaction()
        {
            using (var fibonacci = new OutputFibonacciToDatabase(TestDatabase.ConnectionStringName, 25, Should.Throw))
            {
                fibonacci.Execute();
                Assert.Single(new List<Exception>(fibonacci.GetAllErrors()));
            }

            AssertFibonacciTableEmpty();
        }
    }
}
