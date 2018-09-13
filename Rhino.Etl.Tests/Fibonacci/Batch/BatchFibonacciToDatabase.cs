#if FEATURE_SQLCOMMANDSET
namespace Rhino.Etl.Tests.Fibonacci.Batch
{
    public class BatchFibonacciToDatabase : BatchFibonacciToDatabaseBase
    {
        public BatchFibonacciToDatabase() : base("test")
        {
        }
    }
}
#endif