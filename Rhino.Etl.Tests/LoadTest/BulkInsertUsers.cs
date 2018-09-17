namespace Rhino.Etl.Tests.LoadTest
{
    using Rhino.Etl.Core.Operations;

    public class BulkInsertUsers : SqlBulkInsertOperation
    {
        public BulkInsertUsers(string connectionStringName)
            : base(connectionStringName, "Users")
        {
        }

        /// <summary>
        /// Prepares the schema of the target table
        /// </summary>
        protected override void PrepareSchema()
        {
            Schema["name"] = typeof (string);
            Schema["email"] = typeof (string);
            Schema["testMsg"] = typeof(string);
        }
    }
}