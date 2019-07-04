namespace Rhino.Etl.Tests.Integration
{
    using System.Configuration;
    using Rhino.Etl.Core;

    public class UsersToPeople : EtlProcess
    {
        private readonly ReadUsers _readUsers;
        private readonly WritePeople _writePeople;

        public UsersToPeople(string connectionStringName)
        {
            _readUsers = new ReadUsers(connectionStringName);
            _writePeople = new WritePeople(connectionStringName);
        }

        public UsersToPeople(ConnectionStringSettings connectionString)
        {
            _readUsers = new ReadUsers(connectionString);
            _writePeople = new WritePeople(connectionString);
        }

        protected override void Initialize()
        {
            Register(_readUsers);
            Register(new SplitName());
            Register(_writePeople);
        }
    }
}