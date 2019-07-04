namespace Rhino.Etl.Tests.Integration
{
    using System.Configuration;
    using System.Data;
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.Operations;

    public class ReadUsers : InputCommandOperation
    {
        public ReadUsers(string connectionStringName)
            : base(connectionStringName)
        {
        }

        public ReadUsers(ConnectionStringSettings connectionStringSettings)
            : base(connectionStringSettings)
        {
        }

        protected override Row CreateRowFromReader(IDataReader reader)
        {
            return Row.FromReader(reader);
        }

        protected override void PrepareCommand(IDbCommand cmd)
        {
            cmd.CommandText = "SELECT * FROM Users";
        }
    }
}