namespace Rhino.Etl.Tests.Infrastructure
{
    using System.Configuration;
    using System.Data;
    using System.Data.SqlClient;
    using Rhino.Etl.Core.Infrastructure;
    using Xunit;

    public class UseFixture : IClassFixture<TestDatabaseFixture>
    {
        public TestDatabaseFixture TestDatabase { get; }

        public UseFixture(TestDatabaseFixture testDatabase)
        {
            this.TestDatabase = testDatabase;
        }

        [Fact]
        public void SupportsAssemblyQualifiedConnectionTypeNameAsProviderNameInConnectionStringSettings()
        {
            string connectionString = Use.ConnectionString(TestDatabase.ConnectionStringName).ConnectionString;
            ConnectionStringSettings connectionStringSettings = new ConnectionStringSettings("test2", connectionString, typeof(SqlConnection).AssemblyQualifiedName);

            using (IDbConnection connection = Use.Connection(connectionStringSettings))
            {
                Assert.NotNull(connection);
            }
        }
 
        [Fact]
        public void SupportsProviderNameInConnectionStringSettings()
        {
            string connectionString = Use.ConnectionString(TestDatabase.ConnectionStringName).ConnectionString;
            ConnectionStringSettings connectionStringSettings = new ConnectionStringSettings("test2", connectionString, "System.Data.SqlClient");

            using (IDbConnection connection = Use.Connection(connectionStringSettings))
            {
                Assert.NotNull(connection);
            }
        }
    }
}
