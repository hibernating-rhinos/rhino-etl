namespace Rhino.Etl.Tests.Dsl
{
    using System.Collections.Generic;
    using System.Data;
    using Rhino.Etl.Core.Infrastructure;
    using Xunit;

    public class JoinFixture : BaseUserToPeopleDslTest
    {
        public JoinFixture(DslTestDatabaseFixture testDatabase) 
            : base(testDatabase)
        { }

        [Fact]
        public void CanCompile()
        {
            using (var process = CreateDslInstance("Dsl/InnerJoin.boo"))
            {
                Assert.NotNull(process);
            }
        }

        [Fact]
        public void CanWriteJoinsToDatabase()
        {
            using (var process = CreateDslInstance("Dsl/InnerJoin.boo"))
            {
                process.Execute();
            }


            List<string> roles = new List<string>();
            Use.Transaction(TestDatabase.ConnectionString, cmd =>
            {
                cmd.CommandText = @"
                    SELECT Roles FROM Users
                    WHERE Roles IS NOT NULL
                    ORDER BY Id
                ";
                using(IDataReader reader = cmd.ExecuteReader())
                while(reader.Read())
                {
                    roles.Add(reader.GetString(0));
                }
            });
            Assert.Equal("ayende rahien is: [admin, janitor, employee, customer]", roles[0]);
            Assert.Equal("foo bar is: [janitor]", roles[1]);
            Assert.Equal("gold silver is: [janitor, employee]", roles[2]);
        }
    }
}