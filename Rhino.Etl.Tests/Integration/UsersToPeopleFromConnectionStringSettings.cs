using System.Configuration;

namespace Rhino.Etl.Tests.Integration
{
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.Infrastructure;

    public class UsersToPeopleFromConnectionStringSettings : EtlProcess
    {
        protected override void Initialize()
        {
            // Get the connection string settings for the test database
            ConnectionStringSettings connectionStringSettings = Use.ConnectionString("test");
            Register(new ReadUsers(connectionStringSettings));
            Register(new SplitName());
            Register(new WritePeople(connectionStringSettings));
        }
    }
}