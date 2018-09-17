namespace Rhino.Etl.Tests
{
    using Rhino.Etl.Core.Infrastructure;
    using Xunit;

    [CollectionDefinition(Name, DisableParallelization = true)]
    public class FibonacciTestCollection : ICollectionFixture<TestDatabaseFixture>
    {
        public const string Name = "FibonacciTest";
    }

    [Collection(FibonacciTestCollection.Name)]
    public class BaseFibonacciTest
    {
        public TestDatabaseFixture TestDatabase { get; }

        public BaseFibonacciTest(TestDatabaseFixture testDatabase)
        {
            this.TestDatabase = testDatabase;

            Use.Transaction(TestDatabase.ConnectionStringName, cmd =>
            {
                cmd.CommandText =
                    @"
if object_id('Fibonacci') is not null
    drop table Fibonacci
create table Fibonacci ( id int );
";
                cmd.ExecuteNonQuery();
            });
        }

        protected void Assert25ThFibonacci()
        {
            int max = Use.Transaction(TestDatabase.ConnectionStringName, cmd =>
            {
                cmd.CommandText = "SELECT MAX(id) FROM Fibonacci";
                return (int) cmd.ExecuteScalar();
            });
            Assert.Equal(75025, max);
        }

        protected void AssertFibonacciTableEmpty()
        {
            int count = Use.Transaction(TestDatabase.ConnectionStringName, cmd =>
            {
                cmd.CommandText = "SELECT count(id) FROM Fibonacci";
                return (int) cmd.ExecuteScalar();
            });
            Assert.Equal(0, count);
        }
    }
}