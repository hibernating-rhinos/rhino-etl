namespace Rhino.Etl.Tests
{
    using System;
    using System.Configuration;
    using Rhino.Etl.Core.Infrastructure;

    public class TestDatabaseFixture : IDisposable
    {
        private static readonly ConnectionStringSettings TestSetupConnectionString = new ConnectionStringSettings("etltest",
            @"Data Source=(localdb)\MSSQLLocalDb;Initial Catalog=master;Integrated Security=SSPI;Timeout=30;",
            @"System.Data.SqlClient");

        private readonly string _testDatabaseName;
        private readonly ConnectionStringSettings _connectionString;

        public TestDatabaseFixture()
            : this(null)
        { }

        protected TestDatabaseFixture(string connectionStringName)
        {
            _testDatabaseName = $"etltest_{DateTime.Now:yyyyMMddTHHmm}_{Guid.NewGuid():N}";
            _connectionString = new ConnectionStringSettings(connectionStringName ?? _testDatabaseName,
                $@"Data Source=(localdb)\MSSQLLocalDb;Initial Catalog={_testDatabaseName};Integrated Security=SSPI;Timeout=30;",
                "System.Data.SqlClient");

            CreateTestDatabase(_testDatabaseName);
            ConnectionProvider.Default.ConnectionStrings.Add(_connectionString);
        }

        public ConnectionStringSettings ConnectionString
        {
            get { return _connectionString; }
        }

        public string ConnectionStringName
        {
            get { return _connectionString.Name; }
        }

        public void Dispose()
        {
            DropTestDatabase(_testDatabaseName);
        }

        private static void CreateTestDatabase(string databaseName)
        {
            using (var connection = Use.Connection(TestSetupConnectionString))
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = $"IF DB_ID('{databaseName}') IS NULL CREATE DATABASE {databaseName};";
                cmd.ExecuteNonQuery();
            }
        }

        private static void DropTestDatabase(string databaseName)
        {
            using (var connection = Use.Connection(TestSetupConnectionString))
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = $"ALTER DATABASE {databaseName} SET SINGLE_USER WITH ROLLBACK IMMEDIATE";
                cmd.ExecuteNonQuery();

                cmd.CommandText = $"IF DB_ID('{databaseName}') IS NOT NULL DROP DATABASE {databaseName};";
                cmd.ExecuteNonQuery();
            }
        }
    }
}