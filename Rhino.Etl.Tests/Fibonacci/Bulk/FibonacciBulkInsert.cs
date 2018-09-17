namespace Rhino.Etl.Tests.Fibonacci.Bulk
{
    using System.Configuration;
    using Rhino.Etl.Core.Operations;

    public class FibonacciBulkInsert : SqlBulkInsertOperation
    {
        public FibonacciBulkInsert(string connectionString)
            : base(connectionString, "Fibonacci")
        {
        }

        public FibonacciBulkInsert(ConnectionStringSettings connectionStringSettings)
            : base(connectionStringSettings, "Fibonacci")
        {
        }

        /// <summary>
        /// Prepares the schema of the target table
        /// </summary>
        protected override void PrepareSchema()
        {
            Schema["id"] = typeof (int);
        }
    }
}