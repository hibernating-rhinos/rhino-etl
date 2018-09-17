#if FEATURE_SQLCOMMANDSET
namespace Rhino.Etl.Tests.Fibonacci.Batch
{
    using System.Configuration;
    using System.Data.SqlClient;
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.Operations;

    public class BatchFibonacciToDatabase : SqlBatchOperation
    {
        public BatchFibonacciToDatabase(string connectionStringName)
            : base(connectionStringName)
        {
        }

        public BatchFibonacciToDatabase(ConnectionStringSettings connectionString)
            : base(connectionString)
        {
        }

        /// <summary>
        /// Prepares the command from the given row
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="command">The command.</param>
        protected override void PrepareCommand(Row row, SqlCommand command)
        {
            command.CommandText = "INSERT INTO Fibonacci (id) VALUES(@id)";
            command.Parameters.AddWithValue("@id", row["id"]);
        }
    }
}
#endif
