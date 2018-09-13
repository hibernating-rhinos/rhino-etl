namespace Rhino.Etl.Tests.Fibonacci.Bulk
{
    using Rhino.Etl.Core.Infrastructure;

    public class FibonacciBulkInsertFromConnectionStringSettings : FibonacciBulkInsertBase
    {
        public FibonacciBulkInsertFromConnectionStringSettings()
            : base(Use.ConnectionString("test"))
        {
        }
    }
}