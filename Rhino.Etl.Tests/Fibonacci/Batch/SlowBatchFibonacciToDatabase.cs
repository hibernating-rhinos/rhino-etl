#if FEATURE_SQLCOMMANDSET
namespace Rhino.Etl.Tests.Fibonacci.Batch
{
    using System.Data.SqlClient;
    using Rhino.Etl.Core;

    public class SlowBatchFibonacciToDatabase : BatchFibonacciToDatabase
    {
        public SlowBatchFibonacciToDatabase(string connectionStringName)
            : base(connectionStringName)
        {
            Timeout = 60;
        }

        protected override void PrepareCommand(Row row, SqlCommand command)
        {
            command.CommandText = "WAITFOR DELAY '00:00:02'; INSERT INTO Fibonacci (id) VALUES(@id)";
            command.Parameters.AddWithValue("@id", row["id"]);
        }
    }
}
#endif