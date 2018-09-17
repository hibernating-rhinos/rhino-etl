namespace Rhino.Etl.Tests.LoadTest
{
    using Rhino.Etl.Core.Infrastructure;
    using Xunit;

    /// <summary>
    /// This fixture is here to verify that we can handle large amount of data
    /// without consuming too much memory or crashing
    /// </summary>
    public class LoadTestFixture : BaseUserToPeopleTest
    {
        private const int expectedCount = 5000;
        private readonly int _currentUserCount;

        public LoadTestFixture(TestDatabaseFixture testDatabase)
            : base(testDatabase)
        {
            _currentUserCount = GetUserCount("1 = 1");
            using (PushDataToDatabase push = new PushDataToDatabase(testDatabase.ConnectionStringName, expectedCount))
            {
                push.Execute();
            }
        }

        protected void AssertUpdatedAllRows()
        {
            Assert.Equal(expectedCount + _currentUserCount, GetUserCount("testMsg is not null"));

        }

        private int GetUserCount(string where)
        {
            return Use.Transaction<int>(TestDatabase.ConnectionStringName, cmd =>
            {
                cmd.CommandText = "select count(*) from Users where " + where;
                return (int)cmd.ExecuteScalar();
            });
        }

        [Fact]
        public void CanUpdateAllUsersToUpperCase()
        {
            using (UpperCaseUserNames update = new UpperCaseUserNames(TestDatabase.ConnectionStringName))
            {
                update.RegisterLast(new UpdateUserNames(TestDatabase.ConnectionStringName));
                update.Execute();
            }
            AssertUpdatedAllRows();
        }

#if FEATURE_SQLCOMMANDSET
        [Fact]
        public void CanBatchUpdateAllUsersToUpperCase()
        {
            using (UpperCaseUserNames update = new UpperCaseUserNames(TestDatabase.ConnectionStringName))
            {
                update.RegisterLast(new BatchUpdateUserNames(TestDatabase.ConnectionStringName));
                update.Execute();
            }

            AssertUpdatedAllRows();
        }
#endif

        [Fact]
        public void BulkInsertUpdatedRows()
        {
            if(expectedCount != GetUserCount("1 = 1"))
                return;//ignoring test

            using (UpperCaseUserNames update = new UpperCaseUserNames(TestDatabase.ConnectionStringName))
            {
                update.RegisterLast(new BulkInsertUsers(TestDatabase.ConnectionStringName));
                update.Execute();
            }

            AssertUpdatedAllRows();
        }
    }
}