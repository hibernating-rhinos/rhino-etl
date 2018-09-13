namespace Rhino.Etl.Tests.LoadTest
{
    using System.Data;
    using Rhino.Etl.Core.Infrastructure;
    using Xunit;

    /// <summary>
    /// This fixture is here to verify that we can handle large amount of data
    /// without consuming too much memory or crashing
    /// </summary>
    public class LoadTestFixture : BaseUserToPeopleTest
    {
        private const int expectedCount = 5000;
        private int currentUserCount;

        public LoadTestFixture()
        {
            currentUserCount = GetUserCount("1 = 1");
            using (PushDataToDatabase push = new PushDataToDatabase(expectedCount))
                push.Execute();
        }

        protected void AssertUpdatedAllRows()
        {
            Assert.Equal(expectedCount + currentUserCount, GetUserCount("testMsg is not null"));

        }

        private int GetUserCount(string where)
        {
            return Use.Transaction<int>("test", delegate(IDbCommand command)
            {
                command.CommandText = "select count(*) from Users where " + where;
                return (int)command.ExecuteScalar();
            });
        }

        [Fact]
        public void CanUpdateAllUsersToUpperCase()
        {
            using (UpperCaseUserNames update = new UpperCaseUserNames())
            {
                update.RegisterLast(new UpdateUserNames());
                update.Execute();
            }
            AssertUpdatedAllRows();
        }

#if FEATURE_SQLCOMMANDSET
        [Fact]
        public void CanBatchUpdateAllUsersToUpperCase()
        {
            using (UpperCaseUserNames update = new UpperCaseUserNames())
            {
                update.RegisterLast(new BatchUpdateUserNames());
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

            using (UpperCaseUserNames update = new UpperCaseUserNames())
            {
                update.RegisterLast(new BulkInsertUsers());
                update.Execute();
            }

            AssertUpdatedAllRows();
        }
    }
}