namespace Rhino.Etl.Tests.Dsl
{
    using System.Collections.Generic;
    using System.Data;
    using Rhino.Etl.Core.Infrastructure;
    using Xunit;

    public class InputTimeoutFixture : BaseUserToPeopleDslTest
    {
        public InputTimeoutFixture(DslTestDatabaseFixture testDatabase) 
            : base(testDatabase)
        { }

        [Fact]
        public void CanCompile()
        {
            using (var process = CreateDslInstance("Dsl/InputTimeout.boo"))
            {
                Assert.NotNull(process);
            }
        }

        [Fact]
        public void CanCopyTableWithTimeout()
        {
            using (var process = CreateDslInstance("Dsl/InputTimeout.boo"))
            {
                process.Execute();
            }

            List<string> names = Use.Transaction(TestDatabase.ConnectionString, delegate(IDbCommand cmd)
            {
                List<string> tuples = new List<string>();
                cmd.CommandText = "SELECT firstname from people order by userid";
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tuples.Add(reader.GetString(0));
                    }
                }
                return tuples;
            });
            AssertFullNames(names);
        }
    }
}
