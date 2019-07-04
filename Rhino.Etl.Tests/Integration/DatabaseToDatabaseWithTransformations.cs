namespace Rhino.Etl.Tests.Integration
{
    using System.Collections.Generic;
    using System.Data;
    using Rhino.Etl.Core.Infrastructure;
    using Xunit;
    
    public class DatabaseToDatabaseWithTransformations : BaseUserToPeopleTest
    {
        public DatabaseToDatabaseWithTransformations(TestDatabaseFixture testDatabase)
            : base(testDatabase)
        { }

        [Fact]
        public void CanCopyTableWithTransform()
        {
            using (UsersToPeople process = new UsersToPeople(TestDatabase.ConnectionStringName))
            {
                process.Execute();
            }

            List<string[]> names = Use.Transaction(TestDatabase.ConnectionStringName, cmd =>
            {
                List<string[]> tuples = new List<string[]>();
                cmd.CommandText = "SELECT firstname, lastname from people order by userid";
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    while(reader.Read())
                    {
                        tuples.Add(new string[] { reader.GetString(0), reader.GetString(1) });
                    }
                }
                return tuples;
            });
            AssertNames(names);
        }

        [Fact]
        public void CanCopyTableWithTransformFromConnectionStringSettings()
        {
            using (var process = new UsersToPeople(TestDatabase.ConnectionString))
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