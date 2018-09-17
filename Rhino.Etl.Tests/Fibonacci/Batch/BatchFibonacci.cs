#if FEATURE_SQLCOMMANDSET
namespace Rhino.Etl.Tests.Fibonacci.Batch
{
    using System.Configuration;
    using Rhino.Etl.Core;
    using Rhino.Etl.Tests.Errors;
    using Rhino.Etl.Tests.Fibonacci.Output;

    public class BatchFibonacci : EtlProcess
    {
        private readonly BatchFibonacciToDatabase _batchFibonacciToDatabase;
        private readonly int _max;
        private readonly Should _should;

        public BatchFibonacci(string connectionStringName, int max, Should should)
        {
            _max = max;
            _should = should;
            _batchFibonacciToDatabase = new BatchFibonacciToDatabase(connectionStringName);
        }

        public BatchFibonacci(ConnectionStringSettings connectionString, int max, Should should)
        {
            _max = max;
            _should = should;
            _batchFibonacciToDatabase = new BatchFibonacciToDatabase(connectionString);
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        protected override void Initialize()
        {
            Register(new FibonacciOperation(_max));
            if (_should == Should.Throw)
                Register(new ThrowingOperation());
            Register(_batchFibonacciToDatabase);
        }
    }
}
#endif