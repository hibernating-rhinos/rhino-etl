namespace Rhino.Etl.Tests.Fibonacci.Bulk
{
    using System.Configuration;
    using Rhino.Etl.Core;
    using Rhino.Etl.Tests.Errors;
    using Rhino.Etl.Tests.Fibonacci.Output;

    public class BulkInsertFibonacciToDatabase : EtlProcess
    {
        private readonly int _max;
        private readonly Should _should;
        private readonly FibonacciBulkInsert _fibonacciBulkInsert;

        public BulkInsertFibonacciToDatabase(string connectionStringName, int max, Should should)
        {
            _max = max;
            _should = should;
            _fibonacciBulkInsert = new FibonacciBulkInsert(connectionStringName);
        }

        public BulkInsertFibonacciToDatabase(ConnectionStringSettings connectionString, int max, Should should)
        {
            _max = max;
            _should = should;
            _fibonacciBulkInsert = new FibonacciBulkInsert(connectionString);
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        protected override void Initialize()
        {
            Register(new FibonacciOperation(_max));
            if (_should == Should.Throw)
                Register(new ThrowingOperation());
            Register(_fibonacciBulkInsert);
        }
    }
}