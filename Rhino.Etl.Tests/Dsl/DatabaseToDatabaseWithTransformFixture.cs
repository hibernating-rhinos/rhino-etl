namespace Rhino.Etl.Tests.Dsl
{
    using System.Collections.Generic;
    using System.Data;
    using Rhino.Etl.Core.Infrastructure;
    using Xunit;

    public class DatabaseToDatabaseWithTransformFixture : BaseUserToPeopleDslTest
    {
        public DatabaseToDatabaseWithTransformFixture(DslTestDatabaseFixture testDatabase) 
            : base(testDatabase)
        { }

        [Fact]
        public void CanCompile()
        {
            using (var process = CreateDslInstance("Dsl/UsersToPeople.boo"))
            {
                Assert.NotNull(process);
            }
        }

        [Fact]
        public void CanCopyTableWithTransform()
        {
            using (var process = CreateDslInstance("Dsl/UsersToPeople.boo"))
            {
                process.Execute();
            }

            List<string[]> names = Use.Transaction(TestDatabase.ConnectionStringName, cmd =>
            {
                List<string[]> tuples = new List<string[]>();
                cmd.CommandText = "SELECT firstname, lastname from people order by userid";
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tuples.Add(new string[] { reader.GetString(0), reader.GetString(1) });
                    }
                }
                return tuples;
            });
            AssertNames(names);
        }
    }
}