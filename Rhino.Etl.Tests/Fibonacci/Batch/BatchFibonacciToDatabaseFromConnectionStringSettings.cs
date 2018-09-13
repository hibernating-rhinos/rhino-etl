#if FEATURE_SQLCOMMANDSET
namespace Rhino.Etl.Tests.Fibonacci.Batch
{
    using Rhino.Etl.Core.Infrastructure;

    public class BatchFibonacciToDatabaseFromConnectionStringSettings : BatchFibonacciToDatabaseBase
    {
        public BatchFibonacciToDatabaseFromConnectionStringSettings()
            : base(Use.ConnectionString("test"))
        {
        }
    }
}
#endif