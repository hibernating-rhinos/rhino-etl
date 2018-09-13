namespace Rhino.Etl.Tests
{
    using System;
    using System.Configuration;
    using Rhino.Etl.Core.Infrastructure;

    public class TestDatabaseFixture : IDisposable
    {
        public ConnectionStringSettings ConnectionStringSettings { get; } 

        public TestDatabaseFixture()
        {
            if (!ConnectionProvider.Default.ConnectionStrings.Contains("test"))
            {
                ConnectionProvider.Default.ConnectionStrings.Add(new ConnectionStringSettings("test",
                    @"Data Source=(localdb)\MSSQLLocalDb;Initial Catalog=test;Integrated Security=SSPI;Timeout=30;",
                    @"System.Data.SqlClient.SqlConnection, System.Data, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"));
            }
        }

        public void Dispose()
        {
        }
    }
}